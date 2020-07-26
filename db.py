import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).parent.resolve() / "data.db"


SCHEMA_SCRIPT = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY,
    path        TEXT NOT NULL,
    downloaded  TIMESTAMP NOT NULL,
    created     TIMESTAMP NOT NULL,
    modified    TIMESTAMP NOT NULL,
    hash_sha256 TEXT NOT NULL,
    data        BLOB,
    imported    BOOL DEFAULT FALSE NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS files_unique ON files (path, hash_sha256);
CREATE INDEX IF NOT EXISTS files_path ON files (path);
CREATE INDEX IF NOT EXISTS files_imported ON files (imported);

CREATE TABLE IF NOT EXISTS frames (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL,
    type        TEXT NOT NULL,
    timestamp   TIMESTAMP,
    data_json   TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id)
);
-- CREATE UNIQUE INDEX IF NOT EXISTS frames_unique ON frames (file_id, type, timestamp) WHERE timestamp IS NOT NULL;
CREATE INDEX IF NOT EXISTS frames_type ON frames (type);
CREATE INDEX IF NOT EXISTS frames_timestamp ON frames (timestamp);
"""


def open_db():
    conn = sqlite3.connect(str(DB_FILE), isolation_level=None)
    conn.executescript(SCHEMA_SCRIPT)
    return conn
