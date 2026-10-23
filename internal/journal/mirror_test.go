package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
)

// The mirror protocol: the client against the handler, over real HTTP, held to
// the conformance suite like any other store, with the protocol's own rules
// beside it (the statuses, the write id on the wire, the epoch check, the
// listing's end mark).

// testSource is a MirrorSource of one backend, with an epoch the test sets.
type testSource struct {
	b     Backend
	epoch pos.Epoch
}

func (s *testSource) Journal(node cluster.Node) Backend {
	if node != "n1" {
		return nil
	}
	return s.b
}

func (s *testSource) Epoch(ctx context.Context, node cluster.Node) pos.Epoch { return s.epoch }

// mirrorFactory returns a journal mirrored over HTTP: the node's own local
// store behind the handler, the Mirror client in front of it.
func mirrorFactory(t *testing.T) (*Store, record) {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	back := NewLocal(db.DB, "n1")
	srv := httptest.NewServer(NewMirrorHandler(&testSource{b: back}, t.TempDir(), 0))
	t.Cleanup(srv.Close)
	m := NewMirror(srv.Client(), srv.URL, "n1")
	rec := record{}
	rec.Foreign = func(t *testing.T, seq pos.Seg, body string) {
		t.Helper()
		other := NewStore(m, "n9", 1, Options{Backoff: time.Millisecond})
		if err := other.Create(context.Background(), seq, bytes.NewReader([]byte(body))); err != nil {
			t.Fatal(err)
		}
	}
	return NewStore(m, "n1", 1, Options{Backoff: time.Millisecond}), rec
}

func TestConformanceMirror(t *testing.T) { conformance(t, mirrorFactory) }

// mirrorTest is a handler, its URL and a plain client, for the tests that read
// the protocol itself.
type mirrorTest struct {
	srv    *httptest.Server
	url    string
	client *http.Client
	m      *Mirror // the client backend of node n1's journal
	back   *Local  // the store behind it
}

func newMirrorTest(t *testing.T, epoch pos.Epoch) *mirrorTest {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	back := NewLocal(db.DB, "n1")
	srv := httptest.NewServer(NewMirrorHandler(&testSource{b: back, epoch: epoch}, t.TempDir(), 0))
	t.Cleanup(srv.Close)
	return &mirrorTest{srv: srv, url: srv.URL, client: srv.Client(), m: NewMirror(srv.Client(), srv.URL, "n1"), back: back}
}

// put sends the create the test writes by hand, with the headers it names.
func (x *mirrorTest) put(t *testing.T, path string, headers map[string]string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, x.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := x.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The epoch check: a create whose write id names a strictly older epoch than
// the mirror's view of the writer's lease is refused; one that names the same
// epoch, a newer one, or anything at all when the mirror has no view, is not.
func TestMirrorEpochCheck(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		view, epoch pos.Epoch
		ok          bool
	}{{5, 4, false}, {5, 5, true}, {5, 6, true}, {0, 1, true}, {0, 0, true}} {
		x := newMirrorTest(t, tc.view)
		err := x.m.Create(ctx, 1, body("v"), blob.WriteID{Node: "n1", Epoch: tc.epoch, N: 1})
		if tc.ok != (err == nil) {
			t.Fatalf("view %d, write id epoch %d: got %v, ok %v", tc.view, tc.epoch, err, tc.ok)
		}
		if tc.ok {
			continue
		}
		if !errors.Is(err, blob.ErrExists) {
			t.Fatalf("a refusal is blob.ErrExists to the client, got %v", err)
		}
		// Nothing was written: the number is free, and the store that retries
		// reads nothing back to mistake for its own attempt.
		if _, err := x.back.Head(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("a refused create wrote %v", err)
		}
	}
}

// A number already taken answers 412 with the write id it holds in a header,
// which is what the client's read-back reads.
func TestMirrorTakenAnswersWithTheHeldWriteID(t *testing.T) {
	x := newMirrorTest(t, 0)
	held := blob.WriteID{Node: "n1", Epoch: 1, N: 7}
	if err := x.m.Create(context.Background(), 1, body("one"), held); err != nil {
		t.Fatal(err)
	}
	resp := x.put(t, MirrorPath+"n1/1", map[string]string{
		"If-None-Match": "*",
		WriteIDHeader:   mustJSON(t, blob.WriteID{Node: "n1", Epoch: 1, N: 9}),
	}, "two")
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("status %d, want 412", resp.StatusCode)
	}
	if got := resp.Header.Get(WriteIDHeader); got != mustJSON(t, held) {
		t.Fatalf("the 412 names %q, want the held write id %q", got, mustJSON(t, held))
	}
	if got, err := allOf(context.Background(), NewStore(x.m, "n1", 1, Options{}), 1); err != nil || got != "one" {
		t.Fatalf("the segment changed: %q %v", got, err)
	}
}

// A create is conditional and carries a write id: anything else is refused
// before the body is read.
func TestMirrorRefusesAnUnconditionalOrUnnamedCreate(t *testing.T) {
	x := newMirrorTest(t, 0)
	id := mustJSON(t, blob.WriteID{Node: "n1", Epoch: 1, N: 1})
	if resp := x.put(t, MirrorPath+"n1/1", map[string]string{WriteIDHeader: id}, "v"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a PUT without If-None-Match: status %d, want 400", resp.StatusCode)
	}
	if resp := x.put(t, MirrorPath+"n1/1", map[string]string{"If-None-Match": "*"}, "v"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a PUT without a write id: status %d, want 400", resp.StatusCode)
	}
	if resp := x.put(t, MirrorPath+"bad node!/1", map[string]string{"If-None-Match": "*", WriteIDHeader: id}, "v"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a PUT for a name that is not a node: status %d, want 400", resp.StatusCode)
	}
	if resp := x.put(t, MirrorPath+"n1/0", map[string]string{"If-None-Match": "*", WriteIDHeader: id}, "v"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a PUT for number 0: status %d, want 400", resp.StatusCode)
	}
}

// The listing is the numbers from, one per line, ended by a mark; a from past
// the end is an empty listing.
func TestMirrorListing(t *testing.T) {
	ctx := context.Background()
	x := newMirrorTest(t, 0)
	for _, seq := range []pos.Seg{2, 3, 10} {
		if err := x.m.Create(ctx, seq, body(fmt.Sprint(seq)), blob.WriteID{Node: "n1", Epoch: 1, N: uint64(seq)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		from pos.Seg
		want []pos.Seg
	}{{1, []pos.Seg{2, 3, 10}}, {2, []pos.Seg{2, 3, 10}}, {3, []pos.Seg{3, 10}}, {4, []pos.Seg{10}}, {11, nil}} {
		var got []pos.Seg
		for seq, err := range x.m.List(ctx, tc.from) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, seq)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Fatalf("listing from %d: %v, want %v", tc.from, got, tc.want)
		}
	}
	// The wire form, for the record: numbers on lines, then the mark.
	resp, err := x.client.Get(x.url + MirrorPath + "n1?from=3")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "3\n10\n.\n" {
		t.Fatalf("the listing on the wire is %q", b)
	}
}

// A node that mirrors nothing answers 503, not a silent empty journal.
func TestMirrorRefusesAJournalItDoesNotMirror(t *testing.T) {
	x := newMirrorTest(t, 0)
	resp := x.put(t, MirrorPath+"n9/1", map[string]string{
		"If-None-Match": "*",
		WriteIDHeader:   mustJSON(t, blob.WriteID{Node: "n9", Epoch: 1, N: 1}),
	}, "v")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
}

// A delete is done even when the number is empty, and a missing segment is a
// 404 to a get or a head.
func TestMirrorDeleteAndMissing(t *testing.T) {
	ctx := context.Background()
	x := newMirrorTest(t, 0)
	if err := x.m.Create(ctx, 1, body("v"), blob.WriteID{Node: "n1", Epoch: 1, N: 1}); err != nil {
		t.Fatal(err)
	}
	if err := x.m.Delete(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := x.m.Delete(ctx, 1); err != nil {
		t.Fatal("deleting what is gone:", err)
	}
	if _, err := x.m.Head(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("head after delete: %v", err)
	}
	if _, err := x.m.Get(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if _, err := x.m.GetRange(ctx, 1, -3, 0); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("tail after delete: %v", err)
	}
}
