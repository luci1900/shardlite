-- The sweeper's state: where the pass in progress has got to.
CREATE TABLE sweep (
  id INTEGER PRIMARY KEY CHECK (id = 0),
  cursor TEXT NOT NULL DEFAULT '', -- the last key of the last shard swept in this pass, '' between passes
  deleted INTEGER NOT NULL DEFAULT 0 -- files deleted in this pass
);
INSERT INTO sweep (id) VALUES (0);
