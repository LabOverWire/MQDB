CREATE TABLE IF NOT EXISTS records (
    id      TEXT PRIMARY KEY,
    entity  TEXT NOT NULL,
    data    JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_entity ON records(entity);
