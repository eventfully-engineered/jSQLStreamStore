SELECT
	public.Streams.IdOriginal As StreamId,
	public.Messages.StreamVersion,
	public.Messages.Position,
	public.Messages.Id AS MessageId,
	public.Messages.Created,
	public.Messages.Type,
	public.Messages.JsonMetadata
FROM public.Messages
INNER JOIN public.Streams ON public.Messages.StreamIdInternal = public.Streams.IdInternal
WHERE public.Messages.Position <= ?
ORDER BY public.Messages.Position DESC
LIMIT ?;