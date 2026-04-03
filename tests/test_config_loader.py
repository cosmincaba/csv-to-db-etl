import pytest
from pathlib import Path
from src.config_loader import load_config, validate_config

def test_load_config_file_not_found():
    """Test that load_config raises error for non-existent file."""

    with pytest.raises(FileNotFoundError):
        load_config("configs/non_existent_config.yaml")

def test_load_config_success():
    """Test that load_config successfully loads a valid config file."""

    # ACT - Load the customers config
    config = load_config("configs/customers.yaml")

    # ASSERT - Check structure
    assert 'table' in config
    assert 'input_path' in config
    assert 'validation' in config
    assert config['table'] == 'customers'

def test_validate_config_missing_table():
    """Test that validate_config catches missing 'table' field."""

    # ARRANGE
    invalid_config = {
        'inpput_path': 'data/raw/test.csv',
        'validation': {'required_columns': ['id']}
    }

    # ACT & ASSERT
    with pytest.raises(ValueError, match="missing required field: 'table'"):
        validate_config(invalid_config)

def test_validate_config_missing_validation():
    """Test that validate_config catches missing 'validation' field."""
    
    invalid_config = {
        'table': 'test',
        'input_path': 'data/raw/test.csv'
    }

    with pytest.raises(ValueError, match="missing required field: 'validation'"):
        validate_config(invalid_config)

def test_validate_config_valid():
    """Test that validate_config passes for valid config."""

    # ARRANGE
    valid_config = {
        'table': 'test',
        'input_path': 'data/raw/test.csv',
        'validation': {
            'required_columns': ['id', 'name'],
            'non_null_columns': ['id']
        }
    }

    # ACT & ASSERT - Should not raise any exceptions
    validate_config(valid_config)