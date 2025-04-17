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

def get_next_hour_to_process(conn):
    """Get the next hour to process based on metadata table"""
    try:
        # Check if metadata table exists
        result = conn.execute(f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'main' 
                AND table_name = '{METADATA_TABLE}'
            )
        """).fetchone()[0]

        if not result:
            return None

        # Get the maximum processed hour
        max_hour = conn.execute(f"""
            SELECT MAX(hour_key) 
            FROM {METADATA_TABLE}
        """).fetchone()[0]

        if not max_hour:
            return None

        # Calculate next hour
        last_dt = datetime.strptime(max_hour, "%Y%m%d/%H")
        next_dt = last_dt + timedelta(hours=1)
        return next_dt.strftime("%Y%m%d/%H")

    except duckdb.CatalogException:
        return None

def find_earliest_hour():
    """Find the earliest unprocessed hour in data directory"""
    try:
        dates = []
        for date_dir in os.listdir(DATA_PATH):
            date_path = os.path.join(DATA_PATH, date_dir)
            if os.path.isdir(date_path):
                for hour_dir in sorted(os.listdir(date_path)):
                    hour_path = os.path.join(date_path, hour_dir)
                    if os.path.isdir(hour_path):
                        return f"{date_dir}/{hour_dir}"
        return None
    except FileNotFoundError:
        return None

def process_hour(conn, date_hour):
    """Process a single hour of data"""
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
                # Create table if not exists
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id INTEGER PRIMARY KEY,
                        amount FLOAT,
                        transaction_date DATE,
                        hour INTEGER
                    )
                """)
                
                # Insert data
                conn.execute(f"""
                    INSERT INTO {table}
                    SELECT *
                    FROM read_csv_auto('{file_path}')
                """)
                
                processed = True
            except Exception as e:
                logging.error(f"Failed to process {file_path}: {e}")

    if processed:
        conn.execute(f"""
            INSERT INTO {METADATA_TABLE} (hour_key)
            VALUES (?)
        """, [date_hour])
        logging.info(f"Finished processing hour {date_hour}")
    else:
        logging.info(f"No CSV files processed for {date_hour}")

    return processed

if __name__ == "__main__":
    conn = duckdb.connect(DB_PATH)
    
    # Initialize if empty
    if not conn.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        )
    """).fetchone()[0]:
        logging.info("Initializing database...")
        initialize_db()
    else:
        # Get next hour to process
        next_hour = get_next_hour_to_process(conn)

        # If no processed hours, find earliest available
        if not next_hour:
            next_hour = find_earliest_hour()
        
        if next_hour:
            if process_hour(conn, next_hour):
                print(f"Successfully processed {next_hour}")
            else:
                print(f"No new data in {next_hour}")
        else:
            print("No hours to process")
