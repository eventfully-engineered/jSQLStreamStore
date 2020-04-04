CREATE OR REPLACE FUNCTION idempotentAppend(
    stream_name varchar,
    messages new_message[]
)
RETURNS SETOF append_result
AS $$
DECLARE
    _stream_id integer;
    _latest_stream_version bigint;
    _latest_stream_position bigint;
    message new_message;
    ret append_result;
	conflict record;
BEGIN
	-- https://stackoverflow.com/questions/4069718/postgres-insert-if-does-not-exist-already
	IF NOT EXISTS (SELECT * FROM public.streams WHERE public.streams.name = stream_name) THEN
		INSERT INTO public.streams (name) VALUES (stream_name);
	END IF;

	-- lock?
	SELECT public.streams.id, public.streams.version, public.streams.position
	INTO _stream_id, _latest_stream_version, _latest_stream_position
    FROM public.streams
    WHERE public.streams.name = stream_name;


	FOREACH message in ARRAY messages
	LOOP
	    BEGIN
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
	    EXCEPTION WHEN unique_violation THEN
            SELECT * FROM public.messages INTO conflict WHERE public.messages.id = id;
            IF message.type != conflict.type OR message.data != conflict.data OR message.metadata != conflict.metadata THEN
                RAISE unique_violation USING MESSAGE = 'Duplicate message ID: ' || id;
            END IF;
        END;
	end LOOP;

    -- TODO: could be a function to get last stream message
	SELECT version, position
	INTO _latest_stream_version, _latest_stream_position
	FROM public.messages
	WHERE public.messages.stream_id = _stream_id
	ORDER BY public.messages.Position DESC
	LIMIT 1;

	IF _latest_stream_version IS NULL THEN
		_latest_stream_position := -1;
	END IF;

	UPDATE public.streams
	SET version = _latest_stream_version, position = _latest_stream_position
	WHERE public.streams.id = _stream_id;

    RETURN QUERY SELECT null::bigint, _latest_stream_version, _latest_stream_position;

END;
$$ LANGUAGE plpgsql
VOLATILE;
