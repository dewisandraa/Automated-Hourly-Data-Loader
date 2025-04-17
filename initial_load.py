import duckdb
import os

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DW_PATH = os.path.join(BASE_PATH, 'transactions.duckdb')
METADATA_TABLE = 'processed_hours'

def initialize_db():
    """Create tables and load initial data"""
    with duckdb.connect(DW_PATH) as con:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
                hour_key VARCHAR PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # # Similar CREATE TABLE statements for other tables
        initial_dir = os.path.join(BASE_PATH, 'data/initial')
        # Load initial data
        for file in os.listdir(initial_dir):
            if file.endswith('.csv'):
                table_name = file[:-4]
                file_path = os.path.join(initial_dir, file)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{file_path}');")

# if __name__ == "__main__":
#     initialize_db()