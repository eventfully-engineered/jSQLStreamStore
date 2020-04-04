CREATE OR REPLACE FUNCTION readStreamForwards(
    stream_name varchar,
    stream_version bigint,
    batch_size bigint,
    OUT lastStreamVersion bigint,
    OUT lastStreamPosition bigint,
    OUT messages refcursor
)
AS $$
DECLARE
	_stream_id integer;
BEGIN

    SELECT public.streams.id, public.streams.version, public.streams.position
    INTO _stream_id, lastStreamVersion, lastStreamPosition
    FROM public.streams
    WHERE public.streams.name = stream_name;

    OPEN messages FOR
    SELECT
        public.messages.stream_id,
        public.messages.version,
        public.messages.position,
        public.messages.id AS message_id,
        public.messages.created,
        public.messages.type,
        public.messages.metadata
    FROM public.messages
    INNER JOIN public.streams ON public.messages.stream_id = public.streams.id
    WHERE
        public.messages.stream_id = _stream_id
        AND public.messages.version >= stream_version
    ORDER BY public.messages.position
    LIMIT batch_size;

END;
$$ LANGUAGE plpgsql
VOLATILE;
