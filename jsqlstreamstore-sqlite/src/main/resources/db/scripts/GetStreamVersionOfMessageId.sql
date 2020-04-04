SELECT
	messages.stream_version
FROM messages
INNER JOIN streams ON messages.stream_id = streams.id
WHERE
    streams.Id = ? AND messages.Id = ?
LIMIT 1;
