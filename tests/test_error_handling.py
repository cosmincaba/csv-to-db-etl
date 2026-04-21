import pytest
import pandas as pd
from src.extract import extract_csv, ExtractionError
from src.config_loader import load_config, ConfigError

def test_extract_file_not_found():
    """Test that extract raises FileNotFoundError for non-existent file."""
    with pytest.raises(FileNotFoundError, match="File not found"):
        extract_csv("data/raw/non_existent_file.csv")

def test_extract_empty_file():
    """Test that extract raises ExtractionError for empty file."""
    # Create empty test file
    import os
    os.makedirs("data/test", exist_ok=True)
    open("data/test/empty_file.csv", 'w').close()

    with pytest.raises(ExtractionError, match="empty"):
        extract_csv("data/test/empty.csv")

def test_config_file_not_found():
    """Test that load_config raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("config/non_existent_config.yaml")

def test_config_missing_required_field():
    """Test that load_config raises ConfigError for invalid config."""
    # Create invalid config
    import yaml
    import os
    os.makedirs("data/test", exist_ok=True)

    invalid_config = {'table': 'test'} # Missing input_path and validation

    with open("data/test/invalid_config.yaml", 'w') as f:
        yaml.dump(invalid_config, f)
    
    with pytest.raises(ConfigError, match="missing required field"):
        load_config("data/test/invalid_config.yaml")

def test_extract_valid_file_succeeds():
    """Test that extracts succeeds with valid file."""
    df = extract_csv("data/raw/customers.csv")

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert len(df.columns) > 0

def test_config_valid_file_succeeds():
    """Test that load_config succeeds with valid config."""
    config = load_config("configs/customers.yaml")

    assert isinstance(config, dict)
    assert 'table' in config
    assert 'input_path' in config
    assert 'validation' in config