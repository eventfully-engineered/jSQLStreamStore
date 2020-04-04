SELECT
    messages.stream_id,
    messages.version,
    messages.position,
    messages.id AS id,
    messages.created,
    messages.type,
    messages.metadata
FROM messages
INNER JOIN streams ON messages.stream_id = streams.id
WHERE
    messages.stream_id = ?
    AND messages.version <= ?
ORDER BY messages.position DESC
LIMIT ?;
