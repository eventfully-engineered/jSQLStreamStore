SELECT messages.data
FROM messages
WHERE messages.stream_id = ? AND messages.version = ?;
