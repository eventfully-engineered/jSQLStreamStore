DELETE FROM messages
WHERE messages.stream_id = ? AND messages.id = ?;
