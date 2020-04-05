CREATE OR REPLACE FUNCTION deleteStreamExpectedVersion(
    stream_name varchar,
    expected_version bigint
)
RETURNS VOID
AS $$
DECLARE
    _stream_id integer;
    _latest_stream_version bigint;
BEGIN

	SELECT public.streams.id
	INTO _stream_id
    FROM public.streams
    WHERE public.streams.id = stream_name;

	IF _stream_id IS NULL THEN
		RAISE EXCEPTION 'WrongExpectedVersion' USING HINT = 'stream id was null';
	END IF;

    SELECT public.messages.version
    INTO _latest_stream_version
    FROM public.messages
    WHERE public.messages.stream_id = _stream_id
    ORDER BY public.messages.position DESC
    LIMIT 1;

    IF _latest_stream_version != expected_version THEN
--        RAISE EXCEPTION
--        'Wrong expected version: % (Stream: %, Stream Version: %)',
--        write_message.expected_version,
--        write_message.stream_name,
--        _stream_version;
    	RAISE EXCEPTION 'WrongExpectedVersion' USING HINT = 'latest stream version did not match expected stream version';
    END IF;

    DELETE FROM public.messages
    WHERE public.messages.stream_id = _latest_stream_version;

    DELETE FROM public.streams
    WHERE public.streams.id = _stream_id;

    -- TODO: return count?

END;
$$ LANGUAGE plpgsql;
