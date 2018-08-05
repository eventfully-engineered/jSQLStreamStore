CREATE OR REPLACE FUNCTION appendStreamExpectedVersionAny(streamId varchar, streamIdOriginal varchar, newMessages NewMessage[]) RETURNS AppendResult AS $$
DECLARE 
	v_streamIdInternal integer;
	v_metadataStreamId varchar(42);
	v_metadataStreamIdInternal integer;
	latestStreamVersion integer;
	latestStreamPosition bigint;
	message NewMessage;
	currentVersion integer;
	currentPosition bigint;
	--ret record;
	ret AppendResult;
BEGIN
	-- https://stackoverflow.com/questions/4069718/postgres-insert-if-does-not-exist-already
	IF NOT EXISTS (SELECT * FROM public.Streams WHERE public.Streams.Id = streamId) THEN
		INSERT INTO public.Streams (Id, IdOriginal) VALUES (streamId, streamIdOriginal);
	END IF;

	-- lock? such as from eventide
	-- stream_name_hash = hash_64(_stream_name);
    -- PERFORM pg_advisory_xact_lock(stream_name_hash);
	SELECT public.Streams.IdInternal, public.Streams."Version", public.Streams."Position"
	into v_streamIdInternal, latestStreamVersion, latestStreamPosition
    FROM public.Streams
    WHERE public.Streams.Id = streamId;

    -- https://stackoverflow.com/questions/2944297/postgresql-function-for-last-inserted-id
	foreach message in ARRAY newMessages
	loop
		INSERT INTO public.Messages (StreamIdInternal, Id, StreamVersion, Created, Type, JsonData, JsonMetadata)
		SELECT v_streamIdInternal, message.Id, message.StreamVersion + latestStreamVersion + 1, message.Created, message.Type, message.JsonData, message.JsonMetadata;
	end loop;



    -- TODO: may need to fix this
    -- SELECT @latestStreamVersion = MAX(StreamVersion) + @latestStreamVersion + 1
    -- FROM @newMessages
    -- SET @latestStreamVersion = @latestStreamVersion
	SELECT StreamVersion, Position into latestStreamVersion
	FROM public.Messages
	WHERE public.Messages.StreamIDInternal = v_streamIdInternal
	ORDER BY public.Messages.Position DESC
	LIMIT 1;
	
	IF latestStreamPosition IS NULL THEN
		latestStreamPosition := -1;
	END IF;

	UPDATE public.Streams
	SET "Version" = latestStreamVersion, "Position" = latestStreamPosition
	WHERE public.Streams.IdInternal = v_streamIdInternal;

	SELECT latestStreamVersion, latestStreamPosition into currentVersion, currentPosition;
	
	v_metadataStreamId := '\\$\\$' || streamId;		
	
	SELECT IdInternal into v_metadataStreamIdInternal
	FROM public.Streams
	WHERE public.Streams.Id = v_metadataStreamId;
	
	SELECT JsonData, currentVersion, currentPosition into ret
	FROM public.Messages
	WHERE public.Messages.StreamIdInternal = v_metadataStreamIdInternal
	ORDER BY public.Messages.Position DESC
	LIMIT 1;
	
	RETURN ret;

END;
$$ LANGUAGE plpgsql;

