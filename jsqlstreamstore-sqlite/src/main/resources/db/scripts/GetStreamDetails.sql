SELECT
    streams.id,
    streams.version,
    streams.position,
    streams.max_age,
    streams.max_count
FROM streams
WHERE streams.name = ?;
