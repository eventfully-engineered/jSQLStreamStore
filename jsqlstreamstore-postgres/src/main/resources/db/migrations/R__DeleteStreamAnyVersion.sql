CREATE OR REPLACE FUNCTION deleteStreamAnyVersion(
    stream_nme varchar
)
RETURNS INTEGER
AS $$
DECLARE
	_stream_id integer;
	_count integer;
BEGIN

	SELECT public.streams.id
	INTO _stream_id
    FROM public.streams
    WHERE public.streams.name = stream_nme;

    DELETE FROM public.messages
    WHERE public.messages.stream_id = _stream_id;

    DELETE FROM public.streams
    WHERE public.streams.id = _stream_id;
    GET DIAGNOSTICS _count = ROW_COUNT;

    RETURN _count;

END;
$$ LANGUAGE plpgsql;
