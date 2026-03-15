import pandas as pd
from typing import Tuple
from src.logger import setup_logger

logger = setup_logger(__name__)

def validate_dataframe(
    df: pd.DataFrame,
    required_columns: list[str],
    non_null_columns: list[str],
    integer_columns: list[str] = None,
    datetime_columns: list[str] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate a DataFrame according to specified rules.

    Args:
        df: Input DataFrame to validate
        required_columns: Columns that must exist in the DataFrame
        non_null_columns: Columns that cannot have null values
        integer_columns: Columns that must be valid integers
        datetime_columns: Columns that must be valid datetimes strings
    
    Returns:
        Tuple of (valid_df, rejected_df)
        - valid_df: Rows that passed all validations
        - rejected_df: Rows that failed with 'rejection_reason' column
    """

    logger.info("Starting data validation...")
    logger.debug(f"Input DataFrame shape: {df.shape}")

    # Start with all rows marked as valid
    valid_mask = pd.Series([True] * len(df), index=df.index)
    rejection_reasons = pd.Series([""] * len(df), index=df.index)

    # Rule 1: Check required columns exist
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"All required columns are present: {required_columns}")

    # Rule 2: Check for null values in non-null columns
    for col in non_null_columns:
        null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        null_count = null_mask.sum()

        if null_count > 0:
            logger.warning(f"Found {null_count} null/empty values in '{col}'")
            valid_mask = valid_mask & ~null_mask
            rejection_reasons = rejection_reasons.where(
                ~null_mask,
                rejection_reasons + f"Missing {col};"
            )
    logger.info(f"Checked for null values in {len(non_null_columns)} columns")

    # Rule 3: Check integer columns
    if integer_columns:
        for col in integer_columns:
            try:
                # Try to convert to integer
                pd.to_numeric(df[col], errors='coerce')
                non_integer_mask = pd.to_numeric(df[col], errors='coerce').isna()
                non_integer_count = non_integer_mask.sum()

                if non_integer_count > 0:
                    logger.warning(f"Found {non_integer_count} non-integer values in '{col}'")
                    valid_mask = valid_mask & ~non_integer_mask
                    rejection_reasons = rejection_reasons.where(
                        ~non_integer_mask,
                        rejection_reasons + f"Invalid {col} (not an integer);"
                    )
            except Exception as e:
                logger.error(f"Error validating integer column '{col}': {e}")
    
    if integer_columns:
        logger.info(f"Validated integer columns: {integer_columns}")

    # Rule 4: Check datetime columns
    if datetime_columns:
        for col in datetime_columns:
            try:
                # Try to parse as datetime
                pd.to_datetime(df[col], errors='coerce')
                invalid_datetime_mask = pd.to_datetime(df[col], errors='coerce', format='mixed').isna()
                originally_null = df[col].isna()
                invalid_datetime_mask = invalid_datetime_mask & ~originally_null
                invalid_count = invalid_datetime_mask.sum()

                if invalid_count > 0:
                    logger.warning(f"Found {invalid_count} invalid datetime values in '{col}'")
                    valid_mask = valid_mask & ~invalid_datetime_mask
                    rejection_reasons = rejection_reasons.where(
                        ~invalid_datetime_mask,
                        rejection_reasons + f"Invalid {col} format;"
                    )
            except Exception as e:
                logger.error(f"Error validating datetime column '{col}': {e}")
    if datetime_columns:
        logger.info(f"Validated datetime columns: {datetime_columns}")
    
    # Rule 5: Check for duplicate rows
    if len(df) > 0:
        id_column = required_columns[0]  # Assuming first required column is unique ID
        duplicate_mask = df[id_column].duplicated(keep=False)
        duplicate_count = duplicate_mask.sum()

        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate values in '{id_column}'")
            valid_mask = valid_mask & ~duplicate_mask
            rejection_reasons = rejection_reasons.where(
                ~duplicate_mask,
                rejection_reasons + f"Duplicate {id_column};"
            )
        
    logger.info(f"Checked for duplicates in '{id_column}'")
    
    # Split into valid and rejected DataFrames
    valid_df = df[valid_mask].copy()
    rejected_df = df[~valid_mask].copy()

    # Add rejection reason column to rejected rows
    if len(rejected_df) > 0:
        rejected_df["rejection_reason"] = rejection_reasons[~valid_mask].str.rstrip("; ")
    
    # Summary of validation results
    logger.info("=" * 60)
    logger.info("Validation Summary:")
    logger.info(f" Total rows: {len(df)}")
    logger.info(f" Valid rows: {len(valid_df)} ({len(valid_df)/len(df)*100:.1f}%)")
    logger.info(f" Rejected rows: {len(rejected_df)} ({len(rejected_df)/len(df)*100:.1f}%)")
    logger.info("=" * 60)

    return valid_df, rejected_df

def save_rejected_rows(rejected_df: pd.DataFrame, output_path: str) -> None:
    """
    Save rejected rows to a CSV file.

    Args:
        rejected_df: DataFrame containing rejected rows with rejection_reason
        output_path: Path to save the rejected rows CSV
    """
    if len(rejected_df) == 0:
        logger.info("No rejected rows to save")
        return
    
    try:
        rejected_df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(rejected_df)} rejected rows to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save rejected rows: {e}")
        raise


if __name__ == "__main__":
    # Test with sample data
    logger.info("Testing validation with sample data...")

    test_data = pd.DataFrame({
      'customer_id': [1, 2, None, 4, 5, 5],
      'full_name': ['John Doe', 'Jane Smith', 'Bob Wilson', '', 'Alice Brown', 'Charlie'],
      'email': ['john@email.com', 'jane@email.com', 'bob@email.com', 'test@email.com', 'alice@email.com', 'charlie@email.com'],
      'created_at': ['2024-01-15 10:30:00', '2024-01-16 14:22:00', 'invalid-date', '2024-01-18', '2024-01-19', '2024-01-20']
    })

    print("\nTest DataFrame:")
    print(test_data)

    valid_df, rejected_df = validate_dataframe(
        df=test_data,
        required_columns=['customer_id', 'full_name', 'email', 'created_at'],
        non_null_columns=['customer_id', 'full_name', 'email', 'created_at'],
        integer_columns=['customer_id'],
        datetime_columns=['created_at']
    )

    print("\nValid rows:")
    print(valid_df)

    print("\nRejected rows:")
    print(rejected_df)

    if len(rejected_df) > 0:
        save_rejected_rows(rejected_df, "data/processed/test_rejected.csv")