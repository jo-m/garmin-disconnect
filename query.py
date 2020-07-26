#!/usr/bin/env python3

from db import open_db, create_update_views
import sqlite3
import pandas as pd
import json


def devices(conn: sqlite3.Connection):
    query = "SELECT * FROM devices"
    for file_id, data, _ in conn.execute(query):
        yield json.loads(data)


def _get_row(conn: sqlite3.Connection, file_id: int, frame_type: str) -> dict:
    cur = conn.execute(
        "SELECT data_json FROM frames WHERE file_id = ? AND type = ?", (file_id, frame_type)
    )
    rows = list(cur.fetchall())
    assert len(rows) == 1
    return json.loads(rows[0][0])


def activity(conn: sqlite3.Connection, file_id: int):
    # metadata
    # records
    # laps
    # starts/stops
    meta = _get_row(conn, file_id, "file_id")
    settings = _get_row(conn, file_id, "device_settings")
    sport = _get_row(conn, file_id, "sport")
    user = _get_row(conn, file_id, "user_profile")

    records = []
    cur = conn.execute(
        "SELECT timestamp, data_json FROM frames WHERE file_id = ? AND type = 'record'", (file_id,)
    )
    for timestamp, data in cur.fetchall():
        data = json.loads(data)
        data["timestamp"] = timestamp
        records.append(data)
    records = pd.DataFrame(records)
    print(records)

    cur.close()
    laps = []
    cur = conn.execute(
        "SELECT timestamp, data_json FROM frames WHERE file_id = ? AND type = 'lap'", (file_id,)
    )
    for timestamp, data in cur.fetchall():
        data = json.loads(data)
        data["end_time"] = timestamp
        laps.append(data)
    laps = pd.DataFrame(laps)
    print(laps)


create_update_views()
devices(open_db())
activity(open_db(), file_id=98)
