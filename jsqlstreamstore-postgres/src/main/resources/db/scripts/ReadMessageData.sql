SELECT public.messages.data
FROM public.messages
WHERE public.messages.stream_id = ? AND public.messages.version = ?;
