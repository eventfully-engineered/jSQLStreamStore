SELECT
	streams.id As stream_id,
	streams.name As stream_name,
	messages.version,
	messages.position,
	messages.id AS message_id,
	messages.created,
	messages.type,
	messages.metadata
FROM messages
INNER JOIN streams ON messages.stream_id = streams.id
WHERE messages.position >= ?
ORDER BY messages.position
limit ?;
