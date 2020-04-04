SELECT
	streams.id As stream_id,
	streams.name As stream_name,
	messages.version,
	messages.position,
	messages.id AS message_id,
	messages.created,
	messages.type,
	messages.metadata,
    Messages.data
FROM messages
INNER JOIN streams ON Messages.stream_id = streams.id
WHERE messages.Position <= ?
ORDER BY messages.Position DESC
LIMIT ?;
