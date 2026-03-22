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
    rows_upserted = 0
    rows_inserted = 0
    rows_updated = 0

    try:
        df_to_load = df.copy()

        # Convert float columns back to int where applicable, pandas convert int to float when there are nulls
        for col in df_to_load.columns:
            if df_to_load[col].dtype == 'float64':
                if df_to_load[col].notna().all() and (df_to_load[col] == df_to_load[col].astype(int)).all():
                    df_to_load[col] = df_to_load[col].astype(int) 
                    logger.debug(f"Converted column '{col}' from float to int")

        with psycopg.connect(cfg.dsn) as conn:
            with conn.cursor() as cur:
                # Get primary key for this table
                primary_key = get_primary_key(cur, table_name)
                logger.debug(f"Primary key for {table_name} is {primary_key}")

                # Loop through each row in the DataFrame
                for index, row in df_to_load.iterrows():
                    # Build INSERT statement dynamically
                    columns = list(df_to_load.columns)
                    placeholders = ', '.join(['%s'] * len(row))
                    column_names = ', '.join(columns)

                    # Build the UPDATE SET clause for upsert
                    update_columns = [col for col in columns if col != primary_key]
                    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                    # Check if row exists (for accurate logging)
                    check_query = f"SELECT 1 FROM {table_name} WHERE {primary_key} = %s"
                    cur.execute(check_query, (row[primary_key],))
                    exists = cur.fetchone() is not None
                    
                    # UPSERT query
                    query = f"""
                        INSERT INTO {table_name} ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT ({primary_key}) 
                        DO UPDATE SET {update_set}
                    """

                    # Execute the upsert
                    cur.execute(query, tuple(row.values))
                    rows_upserted += 1

                    # Track inserts vs updateds based on pre-check
                    if exists:
                        rows_updated += 1
                    else:
                        rows_inserted += 1
                    
                    # Log progress every 100 rows
                    if rows_upserted % 100 == 0:
                        logger.debug(f"Upserted {rows_upserted} rows so far...")


                conn.commit()
    
        logger.info(f"OK Upserted {rows_upserted} rows into {table_name} table")
        logger.info(f"Inserted: {rows_inserted} new rows")
        logger.info(f"Updated: {rows_updated} existing rows")
        return rows_upserted
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def get_primary_key(cursor, table_name: str) -> str:
    """
    Get the primary key column name for a table.

    Args:
        cursor: Database cursor
        table_name: Name of the table
    
    Returns:
        Primary key column name
    
    Raises:
        ValueError: If no primary key is found
    """
    query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """

    cursor.execute(query, (table_name,))
    result = cursor.fetchone()

    logger.info(f"Query result for {table_name}: {result}")

    if result:
        return result[0]
    else:
        raise ValueError(f"No primary key found for table '{table_name}'")

if __name__ == "__main__":
    logger.info("Testing load_to_db with test data...")

    test_data = pd.DataFrame({
        'customer_id': [999],
        'full_name': ['Test User'],
        'email':['test@email.com'],
        'created_at': [pd.Timestamp.now()]
    })

    load_to_db(test_data, "customers")

    # Test idempotency

    logger.info("\nTesting idempotency - loading same data again...")
    load_to_db(test_data, "customers")

    logger.info("\nTest complete! Check database to verify.")
