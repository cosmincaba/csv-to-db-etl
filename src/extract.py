import pandas as pd
from pathlib import Path
from src.logger import setup_logger

logger = setup_logger(__name__)

class ExtractionError(Exception):
    """Custom exception for extraction errors."""
    pass

def clean_column_name(col: str) -> str:
    """
    Convert column name to snake_case

    Args:
        col: Original column name
    
    Returns:
        Cleaned column name in snake_case
    """
    
    # Replace spaces with underscores
    col = col.replace(" ", "_")

    # Convert to lowercase
    col = col.lower()

    # Remove special characters (keep only alphanumeric and underscores)
    col = ''.join(char if char.isalnum() or char == '_' else '_' for char in col)

    # Remove duplicate underscores
    while "__" in col:
        col = col.replace("__", "_")
    
    # Remove leading/trailing underscores
    col = col.strip("_")

    return col

def extract_csv(file_path: str, delimiter: str = ",") -> pd.DataFrame:
    """
    Read a CSV file and return it as a pandas DataFrame.

    Args:
        file_path: Path to the CSV file
        delimiter: Delimiter used in the CSV file
    
    Returns:
        DataFrame containing the CSV data
    
    Raises:
        FileNotFoundError: If the CSV file does not exist
    """
    logger.info(f"Starting extraction from {file_path}")

    try:
        # Check if file exists
        path = Path(file_path)
        if not path.exists():
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Check if file is readable
        if not path.is_file():
            error_msg = f"Path is not a file: {file_path}"
            logger.error(error_msg)
            raise ExtractionError(error_msg)
    
        # Read CSV
        try:
            df = pd.read_csv(file_path, delimiter=delimiter)
        except pd.errors.EmptyDataError:
            error_msg = f"File is empty: {file_path}"
            logger.error(error_msg)
            raise ExtractionError(error_msg)
        except pd.errors.ParserError as e:
            error_msg = f"Failed to parse CSV: {e}"
            logger.error(error_msg)
            raise ExtractionError(error_msg)
        except Exception as e:
            error_msg = f"Failed to read file: {e}"
            logger.error(error_msg)
            raise ExtractionError(error_msg)
        
        # Check if DataFrame is empty
        if df.empty:
            error_msg = f"No data found in file: {file_path}"
            logger.error(error_msg)
            raise ExtractionError(error_msg)

        # Clean column names
        original_columns = df.columns.tolist()
        df.columns = [clean_column_name(col) for col in df.columns]
        logger.debug(f"Cleaned column names: {original_columns} -> {df.columns.tolist()}")

        logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns from {file_path}")
        return df
    
    except FileNotFoundError:
        raise
    except ExtractionError:
        raise
    except Exception as e:
        error_msg = f"Unexpected error during extraction: {e}"
        logger.error(error_msg)
        raise ExtractionError(error_msg)

if __name__ == "__main__":
    logger.info("Testing extract_csv function...")
    
    # Test 1: Normal file
    try:
        df = extract_csv("data/raw/customers.csv")
        print(f"\n Test 1 passed: Extracted {len(df)} rows")
    except Exception as e:
        print(f"\n Test 1 failed: {e}")
    
    # Test 2: File not found
    try:
        df = extract_csv("data/raw/nonexistent.csv")
        print("\n Test 2 failed: Expected FileNotFoundError")
    except FileNotFoundError:
        print("\n Test 2 passed: FileNotFoundError raised as expected")
    except Exception as e:
        print(f"\n Test 2 failed: Wrong exception {e}")

    # Test 3: Empty file
    import os
    os.makedirs("data/test", exist_ok=True)
    open("data/test/empty.csv", "w").close()  # Create empty file

    try:
        df = extract_csv("data/test/empty.csv")
        print("\n Test 3 failed: Expected ExtractionError for empty file")
    except ExtractionError:
        print("\n Test 3 passed: ExtractionError raised for empty file as expected")
    except Exception as e:
        print(f"\n Test 3 failed: Wrong exception {e}")

    # Test 4: Column name cleaning
    print("\n Test 4: Column name cleaning")
    test_cases = [
        ("Full Name", "full_name"),
        ("Email@Address", "email_address"),
        ("Customer#ID", "customer_id"),
        ("  _name_  ", "name"),
        ("User ID", "user_id"),
    ]

    for input_name, expected in test_cases:
        result = clean_column_name(input_name)
        if result == expected:
            print(f"  Passed: '{input_name}' -> '{result}'")
        else:
            print(f"  Failed: '{input_name}' -> '{result}', expected '{expected}'")
            