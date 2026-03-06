import pandas as pd
import psycopg
from src.config import DBConfig

def load_to_db(df: pd.DataFrame, table_name: str) -> int:
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        df: pandas DataFrame to load
        table_name: Name of the target table

    Returns:
        Number of rows inserted

    """
    cfg = DBConfig()
    rows_inserted = 0

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
    print(f"OK Loaded {rows_inserted} rows into {table_name} table")
    return rows_inserted

if __name__ == "__main__":
    test_data = pd.DataFrame({
        'customer_id': [999],
        'full_name': ['Test User'],
        'email':['test@email.com'],
        'created_at': [pd.Timestamp.now()]
    })

    print("Testing load_to_db with test data...")
    load_to_db(test_data, "customers")
    print("\nCheck your database to see customer_id 999 inserted")