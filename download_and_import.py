#!/usr/bin/env python3

import hashlib
import os
import sqlite3
import typing
import json
import getpass
import datetime
from pathlib import Path
from subprocess import Popen
import fitdecode
from db import open_db, DB_FILE

WATCH_ROOT = Path(f"/media/{getpass.getuser()}/GARMIN")
DOWNLOAD_ROOT = Path(__file__).parent.resolve() / "files"


class File(typing.NamedTuple):
    @classmethod
    def from_path(cls, path_abs: Path):
        path = str(path_abs.relative_to(DOWNLOAD_ROOT))
        st = os.stat(str(path_abs))
        downloaded = datetime.datetime.now().astimezone()
        created = datetime.datetime.fromtimestamp(st.st_ctime).astimezone()
        modified = datetime.datetime.fromtimestamp(st.st_mtime).astimezone()
        with open(str(path_abs), "rb") as f:
            data = f.read()
            hash_sha256 = hashlib.sha256(data).hexdigest()

        return cls(
            id=None,
            path=path,
            path_abs=path_abs,
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
            path=row[1],
            path_abs=DOWNLOAD_ROOT / Path(row[1]),
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
            (self.path, self.hash_sha256),
        )
        return rows.fetchone()[0] > 0

    def insert(self, conn: sqlite3.Connection):
        conn.execute(
            "INSERT INTO files "
            "(path, downloaded, created, modified, hash_sha256, data, imported) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                self.path,
                self.downloaded.isoformat(),
                self.created.isoformat(),
                self.modified.isoformat(),
                self.hash_sha256,
                sqlite3.Binary(self.data),
                False,
            ),
        )

    def mark_imported(self, conn: sqlite3.Connection):
        assert self.id is not None
        conn.execute("UPDATE files " "SET imported = TRUE " "WHERE id = ?", (self.id,))

    id: int
    path: str
    path_abs: Path
    downloaded: datetime.datetime
    created: datetime.datetime
    modified: datetime.datetime
    hash_sha256: str
    data: bytes
    imported: bool


def walk_files():
    for dirpath, _, fnames in os.walk(DOWNLOAD_ROOT):
        dirpath = Path(dirpath)
        for fname in fnames:
            fname = dirpath / Path(fname)
            yield File.from_path(fname)


def update_files_in_db():
    conn = open_db()
    conn.execute("BEGIN")
    for file in walk_files():
        if not file.exists_in_db(conn):
            print(f"Found new file {file.path} (modified {file.modified})")
            file.insert(conn)
    conn.execute("COMMIT")


def download():
    """
    Download current files, and insert them into the database.
    """
    from_ = WATCH_ROOT.resolve() / "GARMIN"
    if not from_.exists():
        print("Skipping download, device not available")
        return

    cmd = ["rsync", "--verbose", "--checksum", "--archive", str(from_) + "/", str(DOWNLOAD_ROOT)]
    print("Downloading files via rsync:")
    Popen(cmd).communicate()
    print("Downloading finished.")


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


def parse_activity(file: File, conn: sqlite3.Connection):
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
        count = parse_activity(file, conn)
        print(f"Imported {count} frames")
        file.mark_imported(conn)
    conn.execute("COMMIT")


if __name__ == "__main__":
    print(f"Device: {WATCH_ROOT}")
    print(f"Database: {DB_FILE}")
    print(f"Download folder: {DOWNLOAD_ROOT}")
    download()
    update_files_in_db()
    import_files()
