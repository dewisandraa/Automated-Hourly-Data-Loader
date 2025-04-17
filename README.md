# Automated Hourly Data Loader

This Python script automates the loading of hourly CSV data into a DuckDB database. It is designed to run periodically (e.g., hourly) using a scheduler such as `cron`.

## Install Requirements
Install the required dependencies: <br>
```pip install duckdb```

## Structure

### Paths and Database
- `DB_PATH`: Path to the DuckDB database file (`transactions.duckdb`).
- `DATA_PATH`: Directory path where hourly data CSV files are stored (structured as `data/delta/YYYYMMDD/HH/`).
- `METADATA_TABLE`: Name of the table used to track processed hours (`processed_hours`).

### Functions

#### `ensure_metadata_table(conn)`
Ensures the existence of a metadata table to track processed hours. If it does not exist, the function creates one with columns:
- `hour_key` (primary key)
- `processed_at` (timestamp of processing)

#### `get_next_hour_to_process(conn)`
Determines the next hour of data to process:
- Checks the last processed hour from the metadata table.
- If none exists, scans the `DATA_PATH` directories to find the earliest available hour.

Returns:
- A string representing the next hour to process (`YYYYMMDD/HH`).
- `None` if no data is available.

#### `process_hour(conn, date_hour)`
Processes data files for a specified hour:
- Splits the provided `date_hour` into date and hour components.
- Reads CSV files from the relevant directory (`data/delta/YYYYMMDD/HH/`).
- Loads data into DuckDB tables, creating tables if they don't exist.
- Inserts the processed hour into the metadata table upon successful processing.

Logs activities and handles exceptions gracefully.

## Execution
When run as the main program:
- Connects to DuckDB.
- Checks if the database is initialized:
  - If not, calls `initialize_db()` (user-defined).
- Determines the next available hour to process and processes it.

## Usage with Cron
To automate execution hourly, add the following line to your cron jobs (`crontab -e`):

```bash
0 * * * * /usr/bin/python3 /path/to/your_script.py >> /path/to/logfile.log 2>&1
```

This setup ensures the script runs every hour, processes new hourly data, and logs output to the specified logfile.