SELECT
	Streams.IdInternal, Streams."Version", Streams."Position"
FROM Streams
WHERE Streams.Id = ?;
