-- Back to nothing. Every start clears the index's rows, and the journal's
-- rows are the record's copy too: only the first version of a database was
-- empty enough for a down to cost nothing.
DROP TABLE segment_chunks;
DROP TABLE segments;
DROP TABLE shards;
