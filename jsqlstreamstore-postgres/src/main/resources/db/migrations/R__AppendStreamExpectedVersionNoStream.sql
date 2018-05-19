CREATE OR REPLACE FUNCTION appendStreamExpectedVersionNoStream(streamId varchar, streamIdOriginal varchar, newMessages NewMessage[]) RETURNS AppendResult AS $$
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

INSERT INTO public.Streams (Id, IdOriginal) VALUES (streamId, streamIdOriginal);
SELECT LASTVAL() into v_streamIdInternal;

foreach message in ARRAY newMessages
loop
	INSERT INTO public.Messages (StreamIdInternal, Id, StreamVersion, Created, Type, JsonData, JsonMetadata)
	SELECT v_streamIdInternal, message.*;
end loop;

SELECT StreamVersion, public.messages.Position into lastestStreamVersion, latestStreamPosition
FROM public.Messages
WHERE public.Messages.StreamIDInternal = v_streamIdInternal
ORDER BY public.Messages.Position DESC
LIMIT 1;

IF lastestStreamVersion IS NULL THEN
	lastestStreamVersion := -1;
END IF;

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