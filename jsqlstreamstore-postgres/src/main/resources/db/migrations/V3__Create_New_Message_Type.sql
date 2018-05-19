CREATE TYPE NewMessage AS (
	Id UUID,
	StreamVersion integer,
	Created timestamp,
	Type VARCHAR(128),
	JsonData VARCHAR,
	JsonMetadata VARCHAR
);
