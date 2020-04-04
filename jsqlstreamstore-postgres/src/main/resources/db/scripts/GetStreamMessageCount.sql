SELECT COUNT (*)
FROM public.messages
WHERE public.messages.stream_id = (
	SELECT public.streams.id
    FROM public.streams
    WHERE public.streams.name = ?
 )
