from src.extract import extract_csv
from src.load import load_to_db

def main():
    """
    Run the complete ETL pipeline.

    """
    print("=" * 50)
    print("Starting ETL pipeline...")
    print("=" * 50)

    # Configuration
    input_file = "data/raw/customers.csv"
    target_table = "customers"

    try:
        # Extract
        print("\n[1/2] Extracting data...")
        df = extract_csv(input_file)

        # Load
        print("\n[2/2] Loading data into database...")
        rows_loaded = load_to_db(df, target_table)

        # Summary
        print("\n" + "=" * 50)
        print(f"OK Pipeline completed successfully!")
        print(f"Rows extracted: {len(df)}")
        print(f"Rows loaded: {rows_loaded}")
        print("=" * 50)

    except Exception as e:
        print("\n" + "=" * 50)
        print(f"ERROR: Pipeline failed: {e}")
        print("=" * 50)
        raise

if __name__ == "__main__":
    main()