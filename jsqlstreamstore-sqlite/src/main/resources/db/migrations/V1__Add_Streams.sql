CREATE TABLE IF NOT EXISTS Streams(
    Id                  CHARACTER(42)                           NOT NULL,
    IdOriginal          VARCHAR(1000)                           NOT NULL,
    IdInternal          INTEGER PRIMARY KEY,
    "Version"           BIGINT              DEFAULT(-1)         NOT NULL,
    "Position" 			BIGINT 				DEFAULT(-1)			NOT NULL
    --CONSTRAINT PK_Streams PRIMARY KEY (IdInternal)
); -- WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS IX_Streams_Id ON Streams (Id);
