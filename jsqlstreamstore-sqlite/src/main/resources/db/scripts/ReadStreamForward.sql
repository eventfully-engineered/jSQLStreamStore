SELECT
    messages.stream_id,
    messages.version,
    messages.position,
    messages.id AS message_id,
    messages.created,
    messages.type,
    messages.metadata
FROM messages
INNER JOIN streams ON messages.stream_id = streams.id
WHERE
    messages.stream_id = _stream_id
    AND messages.version >= stream_version
ORDER BY messages.position
LIMIT batch_size;
