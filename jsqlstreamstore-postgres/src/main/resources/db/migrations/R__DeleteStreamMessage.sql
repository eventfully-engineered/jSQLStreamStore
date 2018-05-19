CREATE OR REPLACE FUNCTION deleteStreamMessage(streamId varchar, messageId UUID) RETURNS integer AS $$
DECLARE
	v_streamIdInternal integer;
	count integer;
BEGIN

	SELECT public.Streams.IdInternal into v_streamIdInternal
	FROM public.Streams
	WHERE public.Streams.Id = streamId;
	
	DELETE FROM public.Messages
    WHERE public.Messages.StreamIdInternal = v_streamIdInternal AND public.Messages.Id = messageId;
    GET DIAGNOSTICS count = ROW_COUNT;
    
    RETURN count;
END;
$$ LANGUAGE plpgsql;