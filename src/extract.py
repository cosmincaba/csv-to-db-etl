import pandas as pd
from pathlib import Path

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
    path = Path(file_path)

    # Check if the file exists
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    # Read the CSV
    df = pd.read_csv(path)

    print(f"OK Extracted {len(df)} rows from {file_path}")

    return df

if __name__ == "__main__":
    df = extract_csv("data/raw/customers.csv")
    print("\nFirst few rows:")
    print(df.head())
    print(f"\nColumns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
