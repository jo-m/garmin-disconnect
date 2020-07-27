# Garmin Disconnect - Open source alternative to Garmin Connect

**WIP**

This allows you to download, store and display data from Garmin watches.
No internet connection required (except for displaying maps from OSM).

Not affiliated with Garmin at all.

Initially inspired by: <https://www.bunniestudios.com/blog/?p=5863>

Only tested with a Garmin Forerunner 235 with metric settings.

## How it works

1. Synchronization (`download_and_import.py`)

The entire filesystem from your device is downloaded and stored into an SQlite database.
Updated/changed files are handled as well. All original data files are retained (we just
store the FIT files directly in the database as binary BLOBs).

This means we will never lose any data (e.g. if you reset your device, or the device runs out
of storage and deletes old activities), and if we later discover we parsed something the
wrong way, we can just parse the original files again.
It is possible to restore the file system contents of your watch at any point in time from the
database (not implemented yet but the data is there).

2. Parsing (`download_and_import.py`)

The files are then parsed using [fitdecode](https://github.com/polyvertex/fitdecode).
We store the subset of the data we are interested in "mostly schemaless" as JSON
objects in the database.

3. Query and display data (`query.py`)

We can then query and display the data.
Currently, `query.py` can list activities, and display single activities at a time.
Export to a map HTML file which can be viewed in a browser is included.

The plan is to add a JSON web API to query.py, so the data can be pulled
from the DB dynamically and displayed in a web UI.

## Installation and Usage

Currently needs rsync for "historical reasons", this will be removed in the future.

```bash
# install, REQUIRES PYTHON 3.8
git clone git@github.com:jo-m/garmin-disconnect.git
cd garmin-disconnect
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

```bash
# download all data
./download_and_import.py

# show list of available activities
./query.py list

# show an activity, write map to map.html
# use the id from the leftmost column of `./query.py list` output here
./query.py 56
```

## To Do
* Remove superfluous rsync step, sync directly from device FS into DB
* Make work for OSes other than Ubuntu (adapt WATCH_ROOT, maybe drop Python 3.8 requirement)
* Add web API to query.py, so data can be viewed in a web/Javascript frontend
* Add web/javascript frontend
* Test with more watch models
* UI and web API for synchronization

If you want to run this on an OS different than Ubuntu 20.04, make sure to:

1. Install Python 3.8
2. Adapt WATCH_ROOT to where the device filesystem is mounted
