SELECT streams.id, streams.version, streams.position
FROM streams
WHERE streams.name = ?;
