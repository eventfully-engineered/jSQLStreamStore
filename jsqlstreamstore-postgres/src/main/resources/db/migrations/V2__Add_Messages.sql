CREATE TABLE IF NOT EXISTS public.Messages(
    StreamIdInternal    INT                                     NOT NULL,
    StreamVersion       INT                                     NOT NULL,
    Position            SERIAL                                  NOT NULL,
    Id                  UUID                                    NOT NULL,
    Created             timestamp                               NOT NULL,
    Type                VARCHAR(128)                            NOT NULL,
    JsonData            VARCHAR                                 NOT NULL,
    JsonMetadata        VARCHAR                                       ,
    CONSTRAINT PK_Messages PRIMARY KEY (Position),
    CONSTRAINT FK_Messages_Streams FOREIGN KEY (StreamIdInternal) REFERENCES public.Streams(IdInternal)
);

CLUSTER public.Messages USING PK_Messages;

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_Position ON public.Messages (Position);

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Id ON public.Messages (StreamIdInternal, Id);

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Revision ON public.Messages (StreamIdInternal, StreamVersion);

CREATE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Created ON public.Messages (StreamIdInternal, Created);
