CREATE OR REPLACE FUNCTION deleteStreamMessage(
    stream_name varchar,
    message_id UUID
)
RETURNS integer
AS $$
DECLARE
    _stream_id integer;
	_count integer;
BEGIN

	SELECT public.streams.id
	INTO _stream_id
	FROM public.streams
	WHERE public.streams.name = stream_name;

	DELETE FROM public.messages
    WHERE public.messages.stream_id = _stream_id AND public.messages.id = message_id;
    GET DIAGNOSTICS _count = ROW_COUNT;

    RETURN _count;
END;
$$ LANGUAGE plpgsql;
