SELECT COUNT (*)
FROM messages
WHERE messages.id = (
	SELECT streams.id
    FROM streams
    WHERE streams.name = ?
)
