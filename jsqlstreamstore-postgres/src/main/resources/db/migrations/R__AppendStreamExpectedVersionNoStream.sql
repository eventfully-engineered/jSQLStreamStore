CREATE OR REPLACE FUNCTION appendStreamExpectedVersionNoStream(
    stream_name varchar,
    messages new_message[]
)
RETURNS SETOF append_result
AS $$
DECLARE
    _stream_id integer;
    _latest_stream_version bigint;
    _latest_stream_position bigint;
    _max_count bigint;
    message new_message;
    ret append_result;
BEGIN

    -- TODO: use returning id?
    INSERT INTO public.streams (name) VALUES (stream_name) RETURNING id INTO _stream_id;

	FOREACH message in ARRAY messages
	LOOP
		INSERT INTO public.messages (
		    id,
		    stream_id,
		    type,
		    version,
		    data,
		    metadata
		)
		VALUES (
		    message.id, -- TODO: might have to create
		    _stream_id,
		    message.type,
		    message.version,
		    message.data,
		    message.metadata
		);
	end LOOP;

    -- TODO: could be a function to get last stream message
	SELECT version, position, max_count
	INTO _latest_stream_version, _latest_stream_position, _max_count
	FROM public.messages
	WHERE public.messages.stream_id = _stream_id
	ORDER BY public.messages.position DESC
	LIMIT 1;

    IF _latest_stream_version IS NULL THEN
        _latest_stream_version := -1;
    END IF;

    IF _latest_stream_position IS NULL THEN
        _latest_stream_position := -1;
    END IF;

	UPDATE public.streams
	SET version = _latest_stream_version, position = _latest_stream_position
	WHERE public.streams.id = _stream_id;

    RETURN QUERY SELECT _max_count, _latest_stream_version, _latest_stream_position;

END;
$$ LANGUAGE plpgsql
VOLATILE;
