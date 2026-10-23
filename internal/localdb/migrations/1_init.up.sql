-- The local database's first version, what the journal and the cache index
-- need (localdb.go: the journal's rows are kept across a restart, the index's
-- are cleared at every start). The tables are STRICT: a value of the wrong
-- type is refused instead of stored as whatever the column's affinity makes
-- of it.

-- A segment of a node's journal (internal/journal/local.go): its number, the
-- write id that created it and its size. A size of 0 is a fence.
CREATE TABLE segments (
  node TEXT NOT NULL,
  seq INTEGER NOT NULL,
  write_id TEXT NOT NULL,
  size INTEGER NOT NULL,
  PRIMARY KEY (node, seq)
) STRICT;

-- A segment's bytes, in fixed-size chunks. The chunk boundaries are at
-- multiples of the chunk size, so a range read finds its chunks from the
-- offset alone and no value is larger than a chunk. The chunk size is part of
-- the format the bytes are stored in: a change to it is a migration that
-- drops the chunks, since nothing else can read them.
CREATE TABLE segment_chunks (
  node TEXT NOT NULL,
  seq INTEGER NOT NULL,
  idx INTEGER NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY (node, seq, idx)
) STRICT;

-- The cache index (internal/node/index.go). Its rows are cleared at every
-- start, so a change to its columns is a migration that changes this table
-- and keeps the journal's.
CREATE TABLE shards (
  name TEXT PRIMARY KEY, path TEXT NOT NULL, generation TEXT NOT NULL, position INTEGER NOT NULL,
  state TEXT NOT NULL, last_used INTEGER NOT NULL, bytes INTEGER NOT NULL,
  size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, wal_size INTEGER NOT NULL, wal_mtime_ns INTEGER NOT NULL, manifest TEXT NOT NULL, etag TEXT NOT NULL,
  crcs BLOB
) STRICT;
CREATE INDEX shards_lru ON shards (state, last_used);
