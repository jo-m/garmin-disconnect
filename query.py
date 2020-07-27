#!/usr/bin/env python3
import sys

from db import open_db, create_update_views
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import json
from collections import ChainMap
import matplotlib
import matplotlib.cm as cm
import folium


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
    records["speed_kph"] = records["speed"] * 3.6

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

    cur.close()
    cur = conn.execute(
        "SELECT timestamp, data_json "
        "FROM frames "
        "WHERE"
        "   file_id = ?"
        "AND type = 'event' "
        "AND json_extract(data_json, '$.event') = 'timer'",
        (file_id,),
    )
    startstop = []
    for timestamp, data in cur.fetchall():
        data = json.loads(data)
        data["timestamp"] = timestamp
        startstop.append(data)
    startstop = pd.DataFrame(startstop)

    return meta, settings, sport, user, records, laps, startstop


def plot_osm_map(track, output_file="map.html"):
    norm = matplotlib.colors.Normalize(
        vmin=min(track["speed_kph"]), vmax=max(track["speed_kph"]), clip=True
    )
    mapper = cm.ScalarMappable(norm=norm, cmap=cm.plasma)
    map_ = folium.Map(
        location=[track["position_lat"][0], track["position_long"][0]], zoom_start=15
    )
    for _, row in track.iterrows():
        tooltip = f'{row["speed_kph"]:0.1f}kmh {row["heart_rate"]}bpm {row["altitude"]:0.1f}m'
        marker = folium.Circle(
            location=(row["position_lat"], row["position_long"]),
            radius=row["speed_kph"] ** 2 / 5,
            tooltip=tooltip,
            fill_color=matplotlib.colors.to_hex(mapper.to_rgba(row["speed_kph"])),
            fill=True,
            fill_opacity=0.5,
            weight=0,
        )
        marker.add_to(map_)
    folium.PolyLine(zip(track["position_lat"], track["position_long"])).add_to(map_)

    map_.save(output_file)


def show_activity(file_id: int):
    create_update_views()
    _, _, _, _, records, laps, startstop = activity(open_db(), file_id)

    print(records)
    print(laps)
    print(startstop)

    plot_osm_map(records, output_file="map.html")
    print("Map written to map.html")

    records.plot.scatter("distance", "altitude")
    records.plot.scatter("distance", "speed")
    records.plot.scatter("distance", "heart_rate")
    plt.show()


def activities(conn: sqlite3.Connection):
    query = "SELECT * FROM activities"
    for file_id, _, all_data in conn.execute(query):
        all_data = dict(ChainMap(*json.loads(all_data)))
        all_data["file_id"] = file_id
        yield all_data


def main():
    if len(sys.argv) != 2:
        print("Usage: query.py <action>")
        print("Usage:     where action is 'list', or an activity id")
        sys.exit(1)

    create_update_views()

    if sys.argv[1] == "list":
        for a in activities(open_db()):
            print(
                f"#{a['file_id']:03d} {a['name']:10s} Time: {a['time_created']}, "
                f"device: {a['manufacturer']} {a['serial_number']}, "
                f"sport: {a['sport']}"
            )
    else:
        file_id = int(sys.argv[1])
        show_activity(file_id)


if __name__ == "__main__":
    main()
