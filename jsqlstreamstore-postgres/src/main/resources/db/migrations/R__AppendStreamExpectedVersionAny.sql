CREATE OR REPLACE FUNCTION appendStreamExpectedVersionAny(streamId varchar, streamIdOriginal varchar, newMessages NewMessage[]) RETURNS AppendResult AS $$
DECLARE 
	v_streamIdInternal integer;
	v_metadataStreamId varchar(42);
	v_metadataStreamIdInternal integer;
	lastestStreamVersion integer;
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
	
	-- lock?
	SELECT public.Streams.IdInternal, public.Streams."Version" into
			v_streamIdInternal, lastestStreamVersion
    FROM public.Streams
    WHERE public.Streams.Id = streamId;
    
	foreach message in ARRAY newMessages
	loop
		INSERT INTO public.Messages (StreamIdInternal, Id, StreamVersion, Created, Type, JsonData, JsonMetadata)
		SELECT v_streamIdInternal, message.*;
	end loop;
	
	SELECT StreamVersion, Position into lastestStreamVersion
	FROM public.Messages
	WHERE public.Messages.StreamIDInternal = v_streamIdInternal
	ORDER BY public.Messages.Position DESC
	LIMIT 1;
	
	IF latestStreamPosition IS NULL THEN
		latestStreamPosition := -1;
	END IF;

	UPDATE public.Streams
	SET "Version" = lastestStreamVersion, "Position" = latestStreamPosition
	WHERE public.Streams.IdInternal = v_streamIdInternal;

	SELECT lastestStreamVersion, latestStreamPosition into currentVersion, currentPosition;
	
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

