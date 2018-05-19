CREATE OR REPLACE FUNCTION deleteStreamAnyVersion(streamId varchar) RETURNS INTEGER AS $$
DECLARE
	v_streamIdInternal integer;
	count integer;
BEGIN

	SELECT @public.Streams.IdInternal into v_streamIdInternal
    FROM public.Streams
    WHERE public.Streams.Id = streamId;
    
    DELETE FROM public.Messages
    WHERE public.Messages.StreamIdInternal = v_streamIdInternal;

    DELETE FROM public.Streams
    WHERE public.Streams.Id = streamId;
    GET DIAGNOSTICS count = ROW_COUNT;
    
    RETURN count;

END;
$$ LANGUAGE plpgsql;