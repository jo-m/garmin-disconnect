#!/usr/bin/env python3

import datetime
import getpass
import hashlib
import json
import os
import sqlite3
import sys
import typing
from pathlib import Path

import fitdecode

from db import open_db, DB_FILE, create_update_views


class File(typing.NamedTuple):
    @classmethod
    def from_path(cls, path: Path, device_root: Path):
        assert not path.is_absolute()
        abs_path = device_root / path
        st = os.stat(str(abs_path))
        downloaded = datetime.datetime.now().astimezone()
        created = datetime.datetime.fromtimestamp(st.st_ctime).astimezone()
        modified = datetime.datetime.fromtimestamp(st.st_mtime).astimezone()
        with open(str(abs_path), "rb") as f:
            data = f.read()
            hash_sha256 = hashlib.sha256(data).hexdigest()

        return cls(
            id=None,
            path=path,
            downloaded=downloaded,
            created=created,
            modified=modified,
            hash_sha256=hash_sha256,
            data=data,
            imported=False,
        )

    @classmethod
    def from_db(cls, row: tuple):
        return cls(
            id=row[0],
            path=Path(row[1]),
            downloaded=datetime.datetime.fromisoformat(row[2]),
            created=datetime.datetime.fromisoformat(row[3]),
            modified=datetime.datetime.fromisoformat(row[4]),
            hash_sha256=row[5],
            data=row[6],
            imported=bool(row[7]),
        )

    def exists_in_db(self, conn: sqlite3.Connection):
        rows = conn.execute(
            "SELECT COUNT(*) FROM files " "WHERE path = ? " "AND hash_sha256 = ?",
            (str(self.path), self.hash_sha256),
        )
        return rows.fetchone()[0] > 0

    def insert(self, conn: sqlite3.Connection, download_seq: int):
        conn.execute(
            "INSERT INTO files "
            "(path, downloaded, created, modified, hash_sha256, data, download_seq, imported) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(self.path),
                self.downloaded.isoformat(),
                self.created.isoformat(),
                self.modified.isoformat(),
                self.hash_sha256,
                sqlite3.Binary(self.data),
                download_seq,
                False,
            ),
        )

    def mark_imported(self, conn: sqlite3.Connection):
        assert self.id is not None
        conn.execute("UPDATE files " "SET imported = TRUE " "WHERE id = ?", (self.id,))

    id: int
    path: Path
    downloaded: datetime.datetime
    created: datetime.datetime
    modified: datetime.datetime
    hash_sha256: str
    data: bytes
    imported: bool


def walk_files(device_root: Path):
    for dirpath, _, fnames in os.walk(str(device_root)):
        dirpath = Path(dirpath)
        for fname in fnames:
            fname = (dirpath / Path(fname)).relative_to(device_root)
            yield File.from_path(fname, device_root)


def download_files(device_root: Path):
    conn = open_db()
    conn.execute("BEGIN")
    max_seq = conn.execute("SELECT MAX(download_seq) FROM files").fetchone()[0]
    if max_seq is None:
        max_seq = 0
    for file in walk_files(device_root):
        if not file.exists_in_db(conn):
            print(f"Found new file {file.path} (modified {file.modified})")
            file.insert(conn, max_seq + 1)
    conn.execute("COMMIT")


def to_python(obj, simplify=True, path=None):
    if path is None:
        path = []

    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        return obj
    if isinstance(obj, list):
        return [to_python(x, simplify, path + [i]) for i, x in enumerate(obj)]
    if isinstance(obj, tuple):
        return tuple(to_python(x, simplify, path + [i]) for i, x in enumerate(obj))
    if callable(obj):
        return obj
    if isinstance(obj, datetime.datetime):
        return obj
    if isinstance(obj, datetime.time):
        return obj
    if isinstance(obj, dict):
        return {k: to_python(v, simplify, path + [k]) for k, v in obj.items()}

    if simplify:
        if isinstance(obj, fitdecode.types.FieldDefinition):
            return None
        if isinstance(obj, fitdecode.types.BaseType):
            return None
        if isinstance(obj, fitdecode.types.FieldType):
            return None
        if isinstance(obj, fitdecode.types.FieldData):
            if obj.field is None:
                return None
            return dict(name=obj.field.name, value=obj.value)

    d = dict(__type=obj.__class__.__module__ + "." + obj.__class__.__name__)
    for k in obj.__slots__:
        d[k] = to_python(getattr(obj, k), simplify, path + [k])
    return d


def simple_frame(frame):
    frame = to_python(frame, simplify=True)
    if frame["def_mesg"]["mesg_type"] is None:
        return None
    d = dict()
    d["$type"] = frame["def_mesg"]["mesg_type"]["name"]
    for f in frame["fields"]:
        if f is None or f["value"] is None:
            continue
        d[f["name"]] = f["value"]
    return d


def json_ser_default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.time):
        return obj.isoformat()
    print(type(obj))
    raise TypeError(obj)


def parse_import_activity(file: File, conn: sqlite3.Connection):
    count = 0
    with fitdecode.FitReader(file.data) as fit:
        for frame in fit:
            if not isinstance(frame, fitdecode.FitDataMessage):
                continue

            frame = simple_frame(frame)
            if frame is None:
                continue
            if "timestamp" in frame:
                timestamp = frame["timestamp"].isoformat()
                del frame["timestamp"]
            else:
                timestamp = None
            type_ = frame["$type"]
            del frame["$type"]
            data = json.dumps(frame, default=json_ser_default)
            conn.execute(
                "INSERT INTO frames "
                "(file_id, type, timestamp, data_json) "
                "VALUES (?, ?, ?, ?)",
                (file.id, type_, timestamp, data),
            )
            count += 1
    return count


def import_files():
    conn = open_db()
    conn.execute("BEGIN")
    rows = conn.execute("SELECT * FROM files " "WHERE path LIKE '%.FIT' " "AND imported = FALSE")
    for row in rows:
        file = File.from_db(row)
        print("Parsing", file.path)
        count = parse_import_activity(file, conn)
        print(f"Imported {count} frames")
        file.mark_imported(conn)
    conn.execute("COMMIT")


def get_device_root():
    device_root = Path(f"/media/{getpass.getuser()}/GARMIN")
    if len(sys.argv) > 1:
        if sys.argv[1] in ["-h", "--help"]:
            print(f"usage: download_and_import.py [device_root={device_root}]")
            sys.exit(0)
        else:
            device_root = Path(sys.argv[1])

    device_file = device_root / "GARMIN" / "DEVICE.FIT"
    if not device_file.exists():
        print(f"No such file: {device_file}, is the device root set correctly?")
        sys.exit(1)

    return device_root


def main():
    device_root = get_device_root()

    print(f"Device root: {device_root}")
    print(f"Database: {DB_FILE}")

    download_files(device_root / "GARMIN")
    import_files()
    create_update_views()


if __name__ == "__main__":
    main()
