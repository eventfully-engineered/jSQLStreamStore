CREATE TABLE IF NOT EXISTS Messages(
    StreamIdInternal    INT                                     NOT NULL,
    StreamVersion       INT                                     NOT NULL,
    Position            INTEGER PRIMARY KEY,                                  --NOT NULL,
    Id                  UUID                                    NOT NULL,
    Created             timestamp                               NOT NULL DEFAULT CURRENT_TIMESTAMP, --DEFAULT (datetime('now','localtime'))
    Type                VARCHAR(128)                            NOT NULL,
    JsonData            VARCHAR                                 NOT NULL,
    JsonMetadata        VARCHAR                                       ,
    -- CONSTRAINT PK_Messages PRIMARY KEY (Position),
    CONSTRAINT FK_Messages_Streams FOREIGN KEY (StreamIdInternal) REFERENCES Streams(IdInternal)
); -- WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_Position ON Messages (Position);

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Id ON Messages (StreamIdInternal, Id);

CREATE UNIQUE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Revision ON Messages (StreamIdInternal, StreamVersion);

CREATE INDEX IF NOT EXISTS IX_Messages_StreamIdInternal_Created ON Messages (StreamIdInternal, Created);

--CREATE TABLE IF NOT EXISTS message_store.messages (
--  id UUID NOT NULL DEFAULT gen_random_uuid(),
--  stream_name text NOT NULL,
--  type text NOT NULL,
--  position bigint NOT NULL,
--  global_position bigserial NOT NULL,
--  data jsonb,
--  metadata jsonb,
--  time TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc') NOT NULL
--);

--CREATE UNIQUE INDEX messages_id ON message_store.messages (
--  id
--);

--CREATE UNIQUE INDEX messages_stream ON message_store.messages (
--  stream_name,
--  position
--);

--CREATE INDEX messages_category ON message_store.messages (
--  message_store.category(stream_name),
--  global_position,
--  message_store.category(metadata->>'correlationStreamName')
--);
