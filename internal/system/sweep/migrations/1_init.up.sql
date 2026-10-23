-- The sweeper's state: where the pass in progress has got to. The table is
-- STRICT: a value of the wrong type is refused instead of stored as whatever
-- the column's affinity makes of it.
CREATE TABLE sweep (
  id INTEGER PRIMARY KEY CHECK (id = 0),
  cursor TEXT NOT NULL DEFAULT '', -- the last key of the last shard swept in this pass, '' between passes
  deleted INTEGER NOT NULL DEFAULT 0 -- files deleted in this pass
) STRICT;
INSERT INTO sweep (id) VALUES (0);
