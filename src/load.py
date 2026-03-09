import pandas as pd
import psycopg
from src.config import DBConfig
from src.logger import setup_logger

logger = setup_logger(__name__)

def load_to_db(df: pd.DataFrame, table_name: str) -> int:
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        df: pandas DataFrame to load
        table_name: Name of the target table

    Returns:
        Number of rows inserted

    """
    logger.info(f"Starting load to table: {table_name}")
    logger.debug(f"DataFrame shape: {df.shape}")

    cfg = DBConfig()
    rows_inserted = 0

    try:
        with psycopg.connect(cfg.dsn) as conn:
            with conn.cursor() as cur:
                # Loop through each row in the DataFrame
                for index, row in df.iterrows():
                    # Build INSERT statement dynamically
                    columns = ', '.join(df.columns)
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                     # Execute the insert
                    cur.execute(query, tuple(row.values))
                    rows_inserted += 1

                # Commit all inserts
                conn.commit()
    
        logger.info(f"OK Loaded {rows_inserted} rows into {table_name} table")
        return rows_inserted
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    logger.info("Testing load_to_db with test data...")

    test_data = pd.DataFrame({
        'customer_id': [999],
        'full_name': ['Test User'],
        'email':['test@email.com'],
        'created_at': [pd.Timestamp.now()]
    })

    load_to_db(test_data, "customers")
