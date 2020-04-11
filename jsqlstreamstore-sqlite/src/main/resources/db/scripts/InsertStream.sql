INSERT OR IGNORE INTO streams(name) VALUES(?);

--INSERT INTO streams(name) VALUES(?)
--ON CONFLICT DO NOTHING;



-- TODO: perf test
--INSERT INTO streams(name) VALUES (?)
-- WHERE NOT EXISTS(SELECT 1 FROM streams WHERE name = ?);

-- https://www.sqlite.org/lang_UPSERT.html
--INSERT INTO vocabulary(word) VALUES('jovial')
--  ON CONFLICT(word) DO UPDATE SET count=count+1;


-- sqlstreamstore
--    INSERT INTO __schema__.streams (id, id_original, max_age, max_count)
--    SELECT _stream_id, _stream_id_original, _max_age, _max_count
--    ON CONFLICT DO NOTHING;
--    GET DIAGNOSTICS _success = ROW_COUNT;
