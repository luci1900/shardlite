-- The cache index (index.go). Every start deletes it, so its schema needs no
-- migrations: a change only changes this file.
CREATE TABLE IF NOT EXISTS shards (
  name TEXT PRIMARY KEY, path TEXT NOT NULL, generation TEXT NOT NULL, position INTEGER NOT NULL,
  state TEXT NOT NULL, last_used INTEGER NOT NULL, bytes INTEGER NOT NULL,
  size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, wal_size INTEGER NOT NULL, wal_mtime_ns INTEGER NOT NULL, manifest TEXT NOT NULL, etag TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS shards_lru ON shards (state, last_used);
