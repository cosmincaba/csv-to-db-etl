import pandas as pd
from pathlib import Path
from src.logger import setup_logger

logger = setup_logger(__name__)

def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file and return it as a pandas DataFrame.

    Args:
        file_path: Path to the CSV file
    
    Returns:
        DataFrame containing the CSV data
    
    Raises:
        FileNotFoundError: If the CSV file does not exist
    """
    logger.info(f"Starting extraction from {file_path}")
    path = Path(file_path)

    # Check if the file exists
    if not path.exists():
        logger.error(f"CSV file not found: {file_path}")
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    try:
        df = pd.read_csv(path)
        logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns from {file_path}")
        logger.debug(f"Columns extracted: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

if __name__ == "__main__":
    logger.info("Testing extract_csv function...")
    df = extract_csv("data/raw/customers.csv")
    print("\nFirst few rows:")
    print(df.head())

