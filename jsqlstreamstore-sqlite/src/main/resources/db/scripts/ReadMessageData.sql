SET @v_streamIdInternal integer;

SELECT public.Streams.IdInternal into @v_streamIdInternal
FROM public.Streams
WHERE public.Streams.Id = $1

SELECT public.Messages.JsonData
FROM public.Messages
WHERE public.Messages.StreamIdInternal = @v_streamIdInternal AND public.Messages.StreamVersion = $2;


/*
Join version -- Might not be correct

SELECT public.Messages.JsonData
FROM public.Messages
INNER JOIN public.Streams ON public.Messages.StreamIdInternal = public.Streams.IdInternal
WHERE public.Streams.Id = $1 AND public.Messages.StreamVersion = $2;
*/
