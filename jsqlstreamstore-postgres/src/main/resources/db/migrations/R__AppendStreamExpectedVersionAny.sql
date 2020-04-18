CREATE OR REPLACE FUNCTION appendStreamExpectedVersionAny(
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
BEGIN
    -- lock? such as from eventide
    -- stream_name_hash = hash_64(_stream_name);
    -- PERFORM pg_advisory_xact_lock(stream_name_hash);
    -- PERFORM acquire_lock(write_message.stream_name);

    -- TODO: function to create stream return id
	-- https://stackoverflow.com/questions/4069718/postgres-insert-if-does-not-exist-already
	IF NOT EXISTS (SELECT * FROM public.streams WHERE public.streams.name = stream_name) THEN
		INSERT INTO public.streams (name) VALUES (stream_name);
	END IF;

    -- TODO: do something different if new stream?
	SELECT public.streams.id, public.streams.version, public.streams.position, public.streams.max_count
	INTO _stream_id, _latest_stream_version, _latest_stream_position, _max_count
    FROM public.streams
    WHERE public.streams.name = stream_name;

    -- https://stackoverflow.com/questions/2944297/postgresql-function-for-last-inserted-id
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
		    message.version + _latest_stream_version + 1,
		    message.data,
		    message.metadata
		);
	end LOOP;

    -- TODO: could be a function to get last stream message
	SELECT version, position
	INTO _latest_stream_version, _latest_stream_position
	FROM public.messages
	WHERE public.messages.stream_id = _stream_id
	ORDER BY public.messages.position DESC
	LIMIT 1;

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
