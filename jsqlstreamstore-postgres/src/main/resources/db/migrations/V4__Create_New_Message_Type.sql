CREATE TYPE new_message AS (
	Id UUID,
	-- stream_name integer,
    type text,
    version bigint,
    -- position bigint,
    data jsonb,
    metadata jsonb,
    created timestamp -- TODO: default?
);
