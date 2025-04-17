import duckdb
import os
from initial_load import initialize_db
from datetime import datetime, timedelta
import logging

DB_PATH = 'transactions.duckdb'
DATA_PATH = 'data/delta'
METADATA_TABLE = 'processed_hours'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# def ensure_metadata_table(conn):
#     conn.execute(f"""
#         CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
#             hour_key VARCHAR PRIMARY KEY,
#             processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#     """)

def get_next_hour_to_process(conn):
    # ensure_metadata_table(conn)
    result = conn.execute(f"SELECT MAX(hour_key) FROM {METADATA_TABLE}").fetchone()[0]
    # all_hours = []
    print(result)
    next_hour = datetime.strptime(result, "%Y%m%d/%H") + timedelta(hours=1)
    # else:
    #     for date in os.listdir(DATA_PATH):
    #         date_path = os.path.join(DATA_PATH, date)
    #         if os.path.isdir(date_path):
    #             for hour in os.listdir(date_path):
    #                 hour_path = os.path.join(date_path, hour)
    #                 if os.path.isdir(hour_path):
    #                     all_hours.append(f"{date}/{hour}")

    #     if not all_hours:
    #         return None

    #     next_hour = min(datetime.strptime(h, "%Y%m%d/%H") for h in all_hours)

    return next_hour.strftime("%Y%m%d/%H")

def process_hour(conn, date_hour):
    date, hour = date_hour.split("/")
    base_path = os.path.join(DATA_PATH, date, hour)

    if not os.path.exists(base_path):
        logging.info(f"No data found for {date_hour}")
        return False

    processed = False
    for file in os.listdir(base_path):
        if file.endswith('.csv'):
            table = file[:-4]
            file_path = os.path.join(base_path, file)

            logging.info(f"Processing {file_path} into table {table}")
            try:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} AS 
                    SELECT * FROM read_csv_auto('{file_path}') LIMIT 0;
                """)
                conn.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM read_csv_auto('{file_path}');
                """)
                processed = True
            except Exception as e:
                logging.error(f"Failed to process {file_path}: {e}")

    if processed:
        conn.execute(f"INSERT INTO {METADATA_TABLE} (hour_key) VALUES (?)", [date_hour])
        logging.info(f"Finished processing hour {date_hour}")
    else:
        logging.info(f"No CSV files processed for {date_hour}")

    return processed

if __name__ == "__main__":
    conn = duckdb.connect(DB_PATH)
    result = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main';").fetchone()[0]
    if not result:
        logging.info("Database is empty. Initializing DB...")
        initialize_db()
    else:
        date_hour_to_process = get_next_hour_to_process(conn)
        if date_hour_to_process:
            if process_hour(conn, date_hour_to_process):
                print(f"Processed {date_hour_to_process}")
            else:
                print(f"No new data to process for {date_hour_to_process}")

