DELETE FROM messages
WHERE messages.stream_id = (
    SELECT id
    FROM streams
    WHERE name = ?
);
