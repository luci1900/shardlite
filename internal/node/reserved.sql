-- The reserved tables (docs/guide.md, Reserved tables), run at every activation
-- through capture's connection, before the app gets the shard. They stay
-- additive with no version, since user_version is the app's, so every statement
-- is safe to run again and commits nothing for a shard that has them. The
-- reminders table is STRICT, so the app's wrong type in due fails its INSERT
-- instead of a wake that never comes.
CREATE TABLE IF NOT EXISTS _shardlite_reminders (
  key TEXT PRIMARY KEY,
  due INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS _shardlite_reminders_due ON _shardlite_reminders (due);
