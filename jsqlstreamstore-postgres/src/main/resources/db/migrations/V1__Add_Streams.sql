CREATE TABLE IF NOT EXISTS public.Streams(
    Id                  CHAR(42)                                NOT NULL,
    IdOriginal          VARCHAR(1000)                           NOT NULL,
    IdInternal          SERIAL                                  NOT NULL,
    "Version"           INT                 DEFAULT(-1)         NOT NULL,
    "Position" 			bigint 				DEFAULT(-1)			NOT NULL,
    CONSTRAINT PK_Streams PRIMARY KEY (IdInternal)
);

CLUSTER public.Streams USING PK_Streams;

CREATE UNIQUE INDEX IF NOT EXISTS IX_Streams_Id ON public.Streams (Id);
