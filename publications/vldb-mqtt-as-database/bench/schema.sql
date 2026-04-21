CREATE TABLE IF NOT EXISTS records (
    id      TEXT PRIMARY KEY,
    entity  TEXT NOT NULL,
    data    JSONB NOT NULL,
    seq     BIGSERIAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_entity ON records(entity);
CREATE INDEX IF NOT EXISTS idx_records_seq ON records(seq);
