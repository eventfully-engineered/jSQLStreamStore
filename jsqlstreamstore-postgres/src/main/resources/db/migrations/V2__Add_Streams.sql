CREATE TABLE IF NOT EXISTS public.streams(
    id serial,
    name text NOT NULL,
    version bigint DEFAULT(-1) NOT NULL,
    position bigint DEFAULT(-1) NOT NULL,
    max_age integer DEFAULT(NULL),
    max_count bigint DEFAULT(NULL)
);

ALTER TABLE public.streams ADD PRIMARY KEY (id) NOT DEFERRABLE INITIALLY IMMEDIATE;

-- CLUSTER public.Streams USING PK_Streams;

CREATE UNIQUE INDEX IF NOT EXISTS streams_name_id ON public.streams (name);
