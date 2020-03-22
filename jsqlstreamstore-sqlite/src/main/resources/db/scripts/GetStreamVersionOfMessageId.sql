SELECT
	Messages.StreamVersion
FROM Messages
INNER JOIN Streams ON Messages.StreamIdInternal = Streams.IdInternal
WHERE
    Streams.Id = ? AND Messages.Id = ?
LIMIT 1;
