import argparse
from pathlib import Path
from src.extract import extract_csv
from src.validate import validate_dataframe, save_rejected_rows
from src.transform import transform_dataframe, save_transformed_data
from src.load import load_to_db
from src.config_loader import load_config, ConfigError
from src.logger import setup_logger
from src.exit_codes import *

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
  python -m src.main --config configs/customers.yaml

  python -m src.main --input data/raw/customers.csv --table customers
        """
    )

    parser.add_argument(
        '--config', '-c',
        type=str,
        help="Path to YAML config file (e.g., configs/customers.yaml)"
    )

    parser.add_argument(
        '--input', '-i',
        type=str,
        help="Path to input CSV file (e.g., data/raw/customers.csv)"
    )

    parser.add_argument(
        '--table', '-t',
        type=str,
        help="Target database table name (e.g., customers)"
    )

    args = parser.parse_args()

    if not args.config and not (args.input and args.table):
        parser.error("Either --config or both --input and --table must be provided.")

    return args


def main():
    """
    Run the complete ETL pipeline.

    """
    args = parse_arguments()

    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)

    try:
        # Load config
        try:
            if args.config:
                logger.info("Running in config-driven mode")
                config = load_config(args.config)

                input_file = config['input_path']
                table_name = config['table']
                validation_config = config['validation']
                output_config = config.get('output', {})

            else:
                logger.warning("Running in command-line mode, consider using --config for better maintainability")
                input_file = args.input
                table_name = args.table

                # Hardcoded validation rules for command-line mode
                if table_name == "customers":
                    validation_config = {
                        'required_columns': ['customer_id', 'full_name', 'email', 'created_at'],
                        'non_null_columns': ['customer_id', 'full_name', 'email', 'created_at'],
                        'integer_columns': ['customer_id'],
                        'datetime_columns': ['created_at']
                    }
                else:
                    logger.error(f"No validation rules defined for table'{table_name}'")
                    return EXIT_CONFIG_ERROR

                output_config = {
                    'rejected_file': f"data/processed/rejected_{table_name}.csv",
                    'clean_file': f"data/processed/clean_{table_name}.csv"
                }

        except FileNotFoundError as e:
            logger.error(f"Configuration file not found: {e}")
            logger.info(f"Exit code: {EXIT_FILE_NOT_FOUND} - {get_exit_code_description(EXIT_FILE_NOT_FOUND)}")
            return EXIT_FILE_NOT_FOUND
        except ConfigError as e:
            logger.error(f"Configuration error: {e}")
            logger.info(f"Exit code: {EXIT_CONFIG_ERROR} - {get_exit_code_description(EXIT_CONFIG_ERROR)}")
            return EXIT_CONFIG_ERROR

        logger.info(f"Input file: {input_file}")
        logger.info(f"Target table: {table_name}")

        # Extract
        logger.info("\n[1/4] Extracting data from CSV...")
        try:
            df = extract_csv(input_file)
        except FileNotFoundError as e:
            logger.error(f"Input file not found: {e}")
            logger.info(f"Exit code: {EXIT_FILE_NOT_FOUND} - {get_exit_code_description(EXIT_FILE_NOT_FOUND)}")
            return EXIT_FILE_NOT_FOUND
        except ExtractionError as e:
            logger.error(f"Data extraction error: {e}")
            logger.info(f"Exit code: {EXIT_FILE_READ_ERROR} - {get_exit_code_description(EXIT_FILE_READ_ERROR)}")
            return EXIT_FILE_READ_ERROR

        # Validate
        logger.info("\n[2/4] Validating data...")
        try:
            valid_df, rejected_df = validate_dataframe(
                df=df,
                required_columns=validation_config['required_columns'],
                non_null_columns=validation_config['non_null_columns'],
                integer_columns=validation_config['integer_columns'],
                datetime_columns=validation_config['datetime_columns']
            )
        except ValueError as e:
            logger.error(f"Data validation error: {e}")
            logger.info(f"Exit code: {EXIT_VALIDATION_ERROR} - {get_exit_code_description(EXIT_VALIDATION_ERROR)}")
            return EXIT_VALIDATION_ERROR

        # Save rejected rows
        if len(rejected_df) > 0:
            rejected_file = output_config.get('rejected_file', f"data/processed/rejected_{table_name}.csv")
            save_rejected_rows(rejected_df, rejected_file)

        # If no valid rows, stop here
        if len(valid_df) == 0:
            logger.error("No valid rows to load into the database. Exiting pipeline.")
            logger.info(f"Exit code: {EXIT_NO_VALID_ROWS} - {get_exit_code_description(EXIT_NO_VALID_ROWS)}")
            return EXIT_NO_VALID_ROWS

        # Transform
        logger.info("\n[3/4] Transforming data...")
        try:
            transformed_df = transform_dataframe(valid_df, table_name)
        except Exception as e:
            logger.error(f"Data transformation error: {e}")
            logger.exception("Full traceback:")
            logger.info(f"Exit code: {EXIT_GENERAL_ERROR} - {get_exit_code_description(EXIT_GENERAL_ERROR)}")
            return EXIT_GENERAL_ERROR

        # Save transformed data
        clean_file = output_config.get('clean_file', f"data/processed/clean_{table_name}.csv")
        save_transformed_data(transformed_df, clean_file)

        # Load
        logger.info("\n[4/4] Loading data into database...")
        try:
            rows_loaded = load_to_db(transformed_df, table_name)
        except Exception as e:
            logger.error(f"Data loading error: {e}")
            logger.exception("Full traceback:")
            logger.info(f"Exit code: {EXIT_DB_LOAD_ERROR} - {get_exit_code_description(EXIT_DB_LOAD_ERROR)}")
            return EXIT_DB_LOAD_ERROR

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info(f"OK Pipeline completed successfully!")
        logger.info(f"Rows extracted: {len(df)}")
        logger.info(f"Rows validated: {len(valid_df)}")
        logger.info(f"Rows rejected: {len(rejected_df)}")
        logger.info(f"Rows transformed: {len(transformed_df)}")
        logger.info(f"Rows loaded: {rows_loaded}")
        logger.info("=" * 60)
        logger.info(f"Exit code: {EXIT_SUCCESS} - {get_exit_code_description(EXIT_SUCCESS)}")

        return EXIT_SUCCESS

    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user.")
        logger.info(f"Exit code: {EXIT_GENERAL_ERROR} - Pipeline interrupted by user")
        return EXIT_GENERAL_ERROR

    except Exception as e:
        logger.critical(f"Pipline failed with error: {e}")
        logger.exception("Full traceback:") # Logs the full error traceback
        logger.info(f"Exit code: {EXIT_GENERAL_ERROR} - {get_exit_code_description(EXIT_GENERAL_ERROR)}")
        return EXIT_GENERAL_ERROR

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
