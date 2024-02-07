CREATE TABLE masks (
    id BIGINT PRIMARY KEY,
    mask BYTEA NOT NULL,
    commitment BYTEA NOT NULL
);
