SELECT
	public.messages.version
FROM public.messages
INNER JOIN public.streams ON public.messages.stream_id = public.streams.id
WHERE
    public.streams.name = ? AND public.messages.id = ?
LIMIT 1;
