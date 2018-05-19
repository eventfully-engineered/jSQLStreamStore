CREATE OR REPLACE FUNCTION readStreamBackwardsWithData(streamId varchar, _streamVersion integer, maxCount integer, OUT lastStreamVersion integer, OUT lastStreamPosition bigint, OUT messages refcursor) AS $$
DECLARE 
    v_streamIdInternal integer;
BEGIN

SELECT 
    public.Streams.IdInternal, public.Streams."Version", public.Streams."Position" 
    into v_streamIdInternal, lastStreamVersion, lastStreamPosition
FROM public.Streams
WHERE public.Streams.Id = streamId;

OPEN messages FOR 
SELECT 
    public.Messages.StreamVersion,
    public.Messages.Position,
    public.Messages.Id AS MessageId,
    public.Messages.Created,
    public.Messages.Type,
    public.Messages.JsonMetadata,
    public.Messages.JsonData
FROM public.Messages
INNER JOIN public.Streams ON public.Messages.StreamIdInternal = public.Streams.IdInternal
WHERE 
    public.Messages.StreamIdInternal = v_streamIdInternal 
    AND public.Messages.StreamVersion <= _streamVersion
ORDER BY public.Messages.Position DESC
LIMIT maxCount;

END;

/*
read stream forwards sql script that doesn't when not used as stored proc
Unsure if there is a way to do this without a proc

SET @v_streamIdInternal integer;
SET @v_lastStreamVersion integer;
SET @v_lastStreamPosition integer;

public.Streams.IdInternal into @v_streamIdInternal, public.Streams."Version" into @v_lastStreamVersion

SELECT 
    public.Streams.IdInternal into @v_streamIdInternal, 
    public.Streams."Version" into @v_lastStreamVersion, 
    public.Streams."Position" into @v_lastStreamPosition
FROM public.Streams
WHERE public.Streams.Id = ?;

/* TODO: return lastStreamPosition */

SELECT 
    public.Messages.StreamVersion,
    public.Messages.Position,
    public.Messages.Id AS MessageId,
    public.Messages.Created,
    public.Messages.Type,
    public.Messages.JsonMetadata,
    public.Messages.JsonData
FROM public.Messages
INNER JOIN public.Streams ON public.Messages.StreamIdInternal = public.Streams.IdInternal
WHERE 
    public.Messages.StreamIdInternal = @v_streamIdInternal 
    AND public.Messages.StreamVersion <= @v_lastStreamVersion
ORDER BY public.Messages.Position DESC
LIMIT ?;
*/

$$ LANGUAGE plpgsql;