SELECT COUNT (*)
FROM public.Messages
WHERE public.Messages.StreamIdInternal = (
	SELECT public.Streams.IdInternal
    FROM public.Streams
    WHERE public.Streams.Id = ?
 )