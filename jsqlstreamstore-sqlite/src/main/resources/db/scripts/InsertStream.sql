INSERT OR IGNORE INTO streams(name) VALUES(?)

-- TODO: perf test
--INSERT INTO streams(name) VALUES (?)
-- WHERE NOT EXISTS(SELECT 1 FROM streams WHERE name = ?);
