import sqlite3
from pathlib import Path
import re

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


VIEWS_SCRIPT = """
CREATE VIEW fit_files AS SELECT id, path, hash_sha256 FROM files WHERE path LIKE '%.FIT';
CREATE VIEW activity_files AS SELECT id, path, hash_sha256 FROM files WHERE path LIKE 'ACTIVITY/%.FIT';
CREATE VIEW monitor_files AS SELECT id, path, hash_sha256 FROM files WHERE path LIKE 'MONITOR/%.FIT';

CREATE VIEW devices AS
    SELECT
        frames.file_id,
        frames.data_json,
        (
            SELECT
                json_group_array(json(device_frames.data_json))
            FROM
                frames as device_frames
            WHERE
                device_frames.file_id = frames.file_id
            AND
                device_frames.type IN ('file_id', 'file_creator', 'software', 'file_capabilities', 'mesg_capabilities', 'field_capabilities')
        ) AS all_data
    FROM frames
    WHERE
        json_extract(frames.data_json, '$.type') = 'device'
    AND frames.type = 'file_id';

CREATE VIEW activities AS
    SELECT
        frames.file_id,
        frames.data_json,
        (
            SELECT
                json_group_array(json(activity_frames.data_json))
            FROM
                frames as activity_frames
            WHERE
                activity_frames.file_id = frames.file_id
            AND
                activity_frames.type IN ('file_id', 'file_creator', 'sport', 'user_profile')
        ) AS all_data
    FROM frames
    WHERE
        frames.type = 'file_id'
    AND
        json_extract(frames.data_json, '$.type') = 'activity';
"""


def open_db():
    conn = sqlite3.connect(str(DB_FILE), isolation_level=None)
    conn.executescript(SCHEMA_SCRIPT)
    return conn


def create_update_views():
    conn = open_db()
    for viewname in re.findall(r"CREATE\s+VIEW\s+([a-zA-Z_]+)", VIEWS_SCRIPT):
        conn.execute(f"DROP VIEW IF EXISTS {viewname}")
    conn.executescript(VIEWS_SCRIPT)
