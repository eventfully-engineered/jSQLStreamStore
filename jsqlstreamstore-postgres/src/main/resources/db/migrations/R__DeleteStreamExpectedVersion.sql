CREATE OR REPLACE FUNCTION deleteStreamExpectedVersion(streamId varchar, expectedStreamVersion integer) RETURNS VOID AS $$
DECLARE
	v_streamIdInternal integer;
	v_latestStreamVersion integer;
BEGIN

	SELECT public.Streams.IdInternal into v_streamIdInternal
    FROM public.Streams
    WHERE public.Streams.Id = streamId;

	IF v_streamIdInternal IS NULL THEN
		RAISE EXCEPTION 'WrongExpectedVersion' USING HINT = 'IdInternal was null';
	END IF;

    SELECT public.Messages.StreamVersion into v_latestStreamVersion
    FROM public.Messages
    WHERE public.Messages.StreamIdInternal = v_streamIdInternal
    ORDER BY public.Messages.Position DESC
    LIMIT 1;
    
    IF v_latestStreamVersion != expectedStreamVersion THEN
    	RAISE EXCEPTION 'WrongExpectedVersion' USING HINT = 'latest stream version did not match expected stream version';
    END IF;

    DELETE FROM public.Messages
    WHERE public.Messages.StreamIdInternal = v_streamIdInternal;

    DELETE FROM public.Streams
    WHERE public.Streams.Id = streamId;

END;
$$ LANGUAGE plpgsql;