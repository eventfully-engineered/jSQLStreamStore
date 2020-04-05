SELECT
	public.streams.id As stream_id,
	public.streams.name As stream_name,
	public.messages.version,
	public.messages.position,
	public.messages.id AS message_id,
	public.messages.created,
	public.messages.type,
	public.messages.metadata,
    public.messages.data
FROM public.messages
INNER JOIN public.streams ON public.messages.stream_id = public.streams.id
WHERE public.messages.position <= ?
ORDER BY public.messages.position DESC
LIMIT ?;
