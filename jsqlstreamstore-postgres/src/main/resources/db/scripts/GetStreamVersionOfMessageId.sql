SELECT
	public.Messages.StreamVersion
FROM public.Messages
INNER JOIN public.Streams ON public.Messages.StreamIdInternal = public.Streams.IdInternal
WHERE
    public.Streams.Id = ? AND public.Messages.Id = ?
LIMIT 1;
