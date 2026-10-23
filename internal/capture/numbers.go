package capture

// The numbers of SQLite's own files: a frame of a WAL, a page of a database,
// a page's size in bytes and a count of pages. They are separate types so that
// the compiler keeps them apart: a page number is not a page count, and a page
// size is not a page number.

// Frame is an index of a frame in a WAL, counted from 1.
type Frame uint32

// Pgno is a page number of a database, counted from 1.
type Pgno uint32

// PageSize is the size of a database page in bytes.
type PageSize uint32

// Pages is the size of a database in pages.
type Pages uint32
