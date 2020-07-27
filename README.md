# View Garmin Device Activity

Tool to download and view data from Garmin GPS watches.

Data can be downloaded and is saved locally in an Sqlite database.

All original data is retained.

Visualization is WIP.

Initially inspired by: <https://www.bunniestudios.com/blog/?p=5863>

Only tested with a Garmin Forerunner 235 with metric settings.

If you want to run this on an OS different than Ubuntu 20.04, make sure to:

1. Install Python 3.8
2. Adapt WATCH_ROOT to where the Watch filesystem is mounted

## Usage

```bash
# download all data
./download_and_import.py

# show list of available activities
./query.py list

# show an activity, write map to map.html
./query.py 56
```
