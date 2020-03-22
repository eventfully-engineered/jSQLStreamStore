SELECT COUNT (*)
FROM Messages
WHERE Messages.StreamIdInternal = (
	SELECT Streams.IdInternal
    FROM Streams
    WHERE Streams.Id = ?
)
