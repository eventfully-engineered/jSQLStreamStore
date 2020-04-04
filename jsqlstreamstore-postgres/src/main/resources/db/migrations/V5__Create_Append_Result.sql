CREATE TYPE append_result AS (
    -- data varchar, I think this is suppose to be max_count?
    max_count bigint,
    current_version bigint,
    current_position bigint
);
