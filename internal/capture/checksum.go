package capture

import (
	"encoding/binary"
	"fmt"
	"hash"

	"github.com/superfly/ltx"
)

// checksumMask is the bits of a checksum that are not its flag.
const checksumMask = ^ltx.ChecksumFlag

// skewMask is what a test hook changes in the capture's checksum, so that a
// snapshot's checksum is not the capture's.
const skewMask = ltx.Checksum(0x5555555555555555)

// Checksums is the CRC of each page of a shard's database and the running
// checksum of the whole database.
//
// A page's CRC is LTX's: the CRC64 of the page number and the page's bytes,
// with LTX's checksum flag on. The checksum of the database is the XOR of the
// pages' CRCs, with the flag on, which is what an LTX file carries before and
// after it is applied.
//
// The pages are held by page number: the first entry is page 1. A page the
// shard has never written has no CRC, and the lock page never has one (LTX
// leaves it out of a file, and out of the checksum with it).
type Checksums struct {
	pages []ltx.Checksum // the CRC of each page, by page number
	sum   ltx.Checksum   // the XOR of the pages' CRCs, without the flag
}

// NewChecksums returns the checksums of an empty database: no pages, and the
// checksum of nothing, which is the flag on its own.
func NewChecksums() *Checksums { return &Checksums{} }

// Checksum is the checksum of the whole database.
func (c *Checksums) Checksum() ltx.Checksum { return ltx.ChecksumFlag | c.sum }

// Pages is how many pages the database has.
func (c *Checksums) Pages() Pages { return Pages(len(c.pages)) }

// Page is the CRC of a page, zero if the page has none.
func (c *Checksums) Page(pgno Pgno) ltx.Checksum {
	if int(pgno) > len(c.pages) || pgno == 0 {
		return 0
	}
	return c.pages[pgno-1]
}

// Set takes a page's CRC: the old one goes out of the running checksum and
// the new one comes in.
func (c *Checksums) Set(pgno Pgno, crc ltx.Checksum) {
	old := c.Page(pgno)
	c.sum = (c.sum ^ (old & checksumMask)) ^ (crc & checksumMask)
	for Pgno(len(c.pages)) < pgno {
		c.pages = append(c.pages, 0)
	}
	c.pages[pgno-1] = crc
}

// Truncate drops the pages past n, which the database no longer has.
func (c *Checksums) Truncate(n Pages) {
	if Pages(len(c.pages)) <= n {
		return
	}
	for _, p := range c.pages[n:] {
		c.sum ^= p & checksumMask
	}
	c.pages = c.pages[:n]
}

// Clone returns a copy of the checksums.
func (c *Checksums) Clone() *Checksums {
	out := &Checksums{sum: c.sum, pages: make([]ltx.Checksum, len(c.pages))}
	copy(out.pages, c.pages)
	return out
}

// MarshalBinary encodes the CRCs of the pages, for the cache index.
func (c *Checksums) MarshalBinary() ([]byte, error) {
	b := make([]byte, 8*len(c.pages))
	for i, p := range c.pages {
		binary.LittleEndian.PutUint64(b[8*i:], uint64(p))
	}
	return b, nil
}

// UnmarshalBinary decodes the CRCs of the pages, as MarshalBinary wrote them.
func (c *Checksums) UnmarshalBinary(b []byte) error {
	if len(b)%8 != 0 {
		return fmt.Errorf("capture: the checksums are %d bytes, not a multiple of 8", len(b))
	}
	c.pages = make([]ltx.Checksum, len(b)/8)
	for i := range c.pages {
		c.pages[i] = ltx.Checksum(binary.LittleEndian.Uint64(b[8*i:]))
	}
	c.sum = 0
	for _, p := range c.pages {
		c.sum ^= p & checksumMask
	}
	return nil
}

// readChecksums reads the CRCs of the pages of the database at path, which
// holds nPages of them. It is the fallback for a shard whose checksums are
// not known: it reads the whole file, once.
func readChecksums(path string, pageSize PageSize, nPages Pages) (*Checksums, error) {
	c := &Checksums{}
	if nPages == 0 {
		return c, nil
	}
	pages := make([]ltx.Checksum, nPages)
	if _, err := ltx.ChecksumPages(path, uint32(pageSize), uint32(nPages), 0, pages); err != nil {
		return nil, fmt.Errorf("capture: read the checksums of %s: %w", path, err)
	}
	c.pages = pages
	for _, p := range c.pages {
		c.sum ^= p & checksumMask
	}
	return c, nil
}

// pending is the CRCs of the pages a file writes, held until the file is
// written: a file that fails leaves the shard's checksums as they were.
//
// A file that holds every page of the database (a snapshot) has its CRCs in
// one slice, indexed by page number. A file that holds some of them has the
// page numbers beside their CRCs.
type pending struct {
	cks    *Checksums     // the checksums the file is written against
	hasher hash.Hash64    // the hasher the pages' CRCs are computed with
	all    bool           // the file holds every page of the database
	pgno   []Pgno         // the pages the file writes, when it is not all of them
	crcs   []ltx.Checksum // the CRC of each page the file writes
}

// newPending is the pending of a file against the checksums cks, which holds
// every page of the database if all is set.
func newPending(cks *Checksums, all bool) *pending {
	return &pending{cks: cks, hasher: ltx.NewHasher(), all: all}
}

// add records the CRC of a page the file writes.
func (p *pending) add(pgno Pgno, crc ltx.Checksum) {
	if p.all {
		for Pgno(len(p.crcs)) < pgno {
			p.crcs = append(p.crcs, 0)
		}
		p.crcs[pgno-1] = crc
		return
	}
	p.pgno = append(p.pgno, pgno)
	p.crcs = append(p.crcs, crc)
}

// after is the checksum of the database once the file's pages are written: the
// pages it writes have their new CRCs, and the pages past the database's new
// end are gone.
func (c *Checksums) after(p *pending, commit Pages) ltx.Checksum {
	if p.all {
		sum := ltx.Checksum(0)
		for _, cr := range p.crcs {
			sum ^= cr & checksumMask
		}
		return ltx.ChecksumFlag | sum
	}
	sum := c.sum
	for i, pgno := range p.pgno {
		sum ^= (c.Page(pgno) & checksumMask) ^ (p.crcs[i] & checksumMask)
	}
	if Pages(len(c.pages)) > commit {
		for _, cr := range c.pages[commit:] {
			sum ^= cr & checksumMask
		}
	}
	return ltx.ChecksumFlag | sum
}

// apply takes the pending CRCs into the checksums: the pages the file wrote
// have their new CRCs, and the pages past the database's new end are gone.
func (c *Checksums) apply(p *pending, commit Pages) {
	if p.all {
		c.pages = p.crcs
		c.sum = 0
		for _, cr := range c.pages {
			c.sum ^= cr & checksumMask
		}
		for Pages(len(c.pages)) < commit {
			c.pages = append(c.pages, 0)
		}
		return
	}
	for i, pgno := range p.pgno {
		c.Set(pgno, p.crcs[i])
	}
	c.Truncate(commit)
}
