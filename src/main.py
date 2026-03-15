import argparse
from pathlib import Path
from src.extract import extract_csv
from src.validate import validate_dataframe, save_rejected_rows
from src.load import load_to_db
from src.logger import setup_logger

logger = setup_logger(__name__)

def parse_arguments():
    """
    Parse command-line arguments for the ETL pipeline.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="ETL Pipeline to load CSV data into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main --input data/raw/customers.csv --table customers
  python -m src.main -i data/raw/customers.csv -t customers
        """
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        required=True,
        help="Path to input CSV file (e.g., data/raw/customers.csv)"
    )
    
    parser.add_argument(
        '--table', '-t',
        type=str,
        required=True,
        help="Target database table name (e.g., customers)"
    )

    return parser.parse_args()


def main():
    """
    Run the complete ETL pipeline.

    """
    args = parse_arguments()

    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    logger.info(f"Input file: {args.input}")
    logger.info(f"Target table: {args.table}")

    try:
        # Extract
        logger.info("\n[1/3] Extracting data from CSV...")
        df = extract_csv(args.input)

        # Validate
        logger.info("\n[2/3] Validating data...")

        if args.table == "customers":
            valid_df, rejected_df = validate_dataframe(
                df=df,
                required_columns=['customer_id', 'full_name', 'email', 'created_at'],
                non_null_columns=['customer_id', 'full_name', 'email', 'created_at'],
                integer_columns=['customer_id'],
                datetime_columns=['created_at']
            )
        else:
            logger.warning(f"No specific validation rules for table '{args.table}', using basic checks...")
            valid_df = df
            rejected_df = df[0:0] # Empty DataFrame
        
        # Save rejected rows
        if len(rejected_df) > 0:
            rejected_file = f"data/processed/rejected_{args.table}.csv"
            save_rejected_rows(rejected_df, rejected_file)
        
        # If no valid rows, stop here

        if len(valid_df) == 0:
            logger.warning("No valid rows to load into the database. Exiting pipeline.")
            return 1

        # Load
        logger.info("\n[3/3] Loading data into database...")
        rows_loaded = load_to_db(valid_df, args.table)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info(f"OK Pipeline completed successfully!")
        logger.info(f"Rows extracted: {len(df)}")
        logger.info(f"Rows validated: {len(valid_df)}")
        logger.info(f"Rows rejected: {len(rejected_df)}")
        logger.info(f"Rows loaded: {rows_loaded}")
        logger.info("=" * 60)

        return 0

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    
    except Exception as e:
        logger.critical(f"Pipline failed with error: {e}")
        logger.exception("Full traceback:") # Logs the full error traceback
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)