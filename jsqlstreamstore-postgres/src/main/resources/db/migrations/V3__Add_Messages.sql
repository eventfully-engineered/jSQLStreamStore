CREATE TABLE IF NOT EXISTS public.messages(
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    stream_id integer NOT NULL,
    type text NOT NULL,
    version bigint NOT NULL,
    position bigserial NOT NULL,
    data jsonb NOT NULL,
    metadata jsonb,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc') NOT NULL
);

-- TODO: do we want to cluster?
-- CLUSTER public.Messages USING PK_Messages;

ALTER TABLE public.messages ADD PRIMARY KEY (position) NOT DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE public.messages ADD CONSTRAINT messages_streams_fk FOREIGN KEY (stream_id) REFERENCES streams (id);

CREATE UNIQUE INDEX IF NOT EXISTS messages_id_key ON public.messages (id);
CREATE UNIQUE INDEX IF NOT EXISTS messages_position_key ON public.messages (Position);
CREATE UNIQUE INDEX IF NOT EXISTS messages_stream_id_version_key ON public.messages (stream_id, version);
CREATE INDEX IF NOT EXISTS messages_stream_id_created_ids ON public.messages (stream_id, created);
