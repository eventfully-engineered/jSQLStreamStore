CREATE TABLE IF NOT EXISTS messages(
    id UUID NOT NULL, -- TODO: possible to do a random_uuid() custom function?
    stream_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    version INTEGER NOT NULL,
    position INTEGER PRIMARY KEY,
    data TEXT NOT NULL,
    metadata TEXT,
    created INTEGER DEFAULT current_timestamp NOT NULL, -- TODO: difference between default (datetime(current_timestamp)) ,
    FOREIGN KEY (stream_id) REFERENCES streams (id)
);

-- ALTER TABLE messages ADD CONSTRAINT messages_streams_fk FOREIGN KEY (stream_id) REFERENCES streams (id);

CREATE UNIQUE INDEX IF NOT EXISTS messages_id_key ON messages (id);
CREATE UNIQUE INDEX IF NOT EXISTS messages_stream_id_version_key ON messages (stream_id, version);
CREATE UNIQUE INDEX IF NOT EXISTS messages_position_key ON messages (position);
CREATE INDEX IF NOT EXISTS messages_stream_id_created_ids ON messages (stream_id, created);
