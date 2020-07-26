#!/usr/bin/env python3

from db import open_db, create_update_views
import sqlite3
import matplotlib.pyplot as plt
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


def to_deg(col: pd.Series):
    return col.astype(float) * (180 / (2 ** 31))


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
    records["position_long"] = to_deg(records["position_long"])
    records["position_lat"] = to_deg(records["position_lat"])
    print(records)
    records.plot.scatter("position_long", "position_lat")
    records.plot.scatter("distance", "altitude")
    records.plot.scatter("distance", "speed")
    records.plot.scatter("distance", "heart_rate")

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
    laps["start_position_lat"] = to_deg(laps["start_position_lat"])
    laps["start_position_long"] = to_deg(laps["start_position_long"])
    laps["end_position_lat"] = to_deg(laps["end_position_lat"])
    laps["end_position_long"] = to_deg(laps["end_position_long"])
    print(laps)

    plt.show()


create_update_views()
devices(open_db())
activity(open_db(), file_id=53)
