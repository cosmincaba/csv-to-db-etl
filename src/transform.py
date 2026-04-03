import pandas as pd
from src.logger import setup_logger

logger = setup_logger(__name__)

def transform_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Transform and clean validated DataFrame.

    """
    logger.info(f"Starting data transformation for table: {table_name}")
    logger.debug(f"Input shape: {df.shape}")

    # Make a copy to avoid modifying original DataFrame
    df_transformed = df.copy()

    # Transformation 1: Table-specific transformations
    if table_name == 'customers':
        df_transformed = transform_customers(df_transformed)
    elif table_name == 'transactions':
        df_transformed = transform_transactions(df_transformed)
    else:
        logger.warning(f"No specific transformations defined for table: {table_name}")

    # Transformation 2: Trim whitespace from string columns
    for col in df_transformed.columns:
        if df_transformed[col].dtype == 'object': # object = string in pandas
            df_transformed[col] = df_transformed[col].str.strip()
            logger.debug(f"Trimmed whitespace from column: {col}")
    
    logger.info ("Trimmed whitespace from string columns")
    logger.info("Data transformation completed")
    logger.debug(f"Output shape: {df_transformed.shape}")

    return df_transformed

def clean_column_name(col: str) -> str:
    """
    Convert column name to snake_case.

    Example: 'Full name' -> 'full_name'
    
    Args:
        col: Original column name
    
    Returns:
        Cleaned column name in snake_case
    """

    # Replace spaces with underscores
    col = col.replace(' ', '_')

    # Convert to lowercase
    col = col.lower()

    # Remove any special characters (keep only alphanumeric and underscores)
    col = ''.join(char if char.isalnum() or char == '_' else '_' for char in col)

    # Remove duplicate underscores
    while '__' in col:
        col = col.replace('__', '_')
    
    # Remove leading/trailing underscores
    col = col.strip('_')

    return col

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply customer-specific transformations.

    Args:
        df: Customers DataFrame
    
    Returns:
        Transformed Customers DataFrame
    """
    logger.info("Applying customer-specific transformations...")

    # Transformation: Title case for names
    if 'full_name' in df.columns:
        df['full_name'] = df['full_name'].str.strip().str.title()
        logger.debug("Applied title case to full_name column")
    
    # Transformation: Lowercase for emails
    if 'email' in df.columns:
        df['email'] = df['email'].str.strip().str.lower()
        logger.debug("Applied lowercase to email column")
    
    # Transformation: Standardize datetime format
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], format = 'mixed')
        logger.debug("Standardized datetime format for created_at column")
    
    logger.info("Customer transformations completed")

    return df

def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transaction-specific transformations.

    Args:
        df: Transactions DataFrame
    
    Returns:
        Transformed Transactions DataFrame
    """
    logger.info("Applying transaction-specific transformations...")

    # Transformation: Uppercase for currency codes
    if 'currency' in df.columns:
        df['currency'] = df['currency'].str.upper()
        logger.debug("Applied uppercase to currency column")
    
    # Transformation: Standardize transaction_date format
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], format = 'mixed')
        logger.debug("Standardized transaction_date format")

    # Transformation: Round amount to 2 decimal places
    if 'amount' in df.columns:
        df['amount'] = df['amount'].round(2)
        logger.debug("Rounded amount to 2 decimal places")
    
    logger.info("Transaction transformations completed")

    return df

def save_transformed_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Save transformed DataFrame to CSV.

    Args:
        df: Transformed DataFrame to save
        output_path: Path to save the cleaned CSV file
    """ 
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} transformed rows to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save transformed data: {e}")
        raise
    
if __name__ == "__main__":
    logger.info("Testing transformation with sample data...")

    test_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'Full Name': ['  JOHN DOE  ', 'jane smith', '  Bob Wilson'],
        'Email': ['JOHN@EMAIL.COM', '  JANE@EMAIL.COM  ', 'Bob@Email.Com'],
        'created_at': ['2024-01-15 10:30:00', '2024-01-16', '2024-01-17 09:15:00']
    })

    print("\nBefore transformation:")
    print(test_data)
    print(f"\nColumns: {list(test_data.columns)}")

    # Transform
    transformed = transform_dataframe(test_data, 'customers')

    print("\nAfter transformation:")
    print(transformed)
    print(f"\nColumns: {list(transformed.columns)}")

    # Save to test file
    save_transformed_data(transformed, 'data/processed/test_clean.csv')