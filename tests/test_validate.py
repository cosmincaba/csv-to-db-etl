import pandas as pd
import pytest
from src.validate import validate_dataframe


def test_validation_catches_missing_columns():
    """ Test that validation raises error when required column is missing."""

    # ARRANGE - Create df missing 'email' column
    df = pd.DataFrame({
        'customer_id': [1, 2],
        'full_name': ['John Doe', 'Jane Doe'],
         'created_at': ['2024-01-01', '2024-01-02']
    })
    
    # ACT & ASSERT - Should raise ValueError for missing 'email' column
    with pytest.raises(ValueError, match="Missing required column"):
        validate_dataframe(
            df=df,
            required_columns=['customer_id', 'full_name', 'email', 'created_at'],
            non_null_columns=['customer_id', 'full_name', 'email']
        )

def test_validation_catches_null_values():
    """Test that validation detects null values in non-nullable columns."""

    # ARRANGE - Create df with null customer_id
    df = pd.DataFrame({
        'customer_id': [1, None, 3],
        'full_name': ['John Doe', 'Jane Doe', 'Jim Carter'],
        'email': ['john.doe@example.com', 'jane.doe@example.com', ' jim.carter@example.com'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
    })

    # ACT - Run validation
    valid_df, rejected_df = validate_dataframe(
        df=df,
        required_columns=['customer_id', 'full_name', 'email', 'created_at'],
        non_null_columns=['customer_id', 'full_name', 'email']
    )

    # ASSERT - Check results
    assert len(valid_df) == 2
    assert len(rejected_df) == 1
    assert rejected_df.iloc[0]['rejection_reason'].startswith('Missing customer_id'), \
        "Rejection reason should mention missing customer_id"

def test_validation_catches_invalid_integer():
    """Test that validation detects non-integer values in integer columns."""

    # ARRANGE - Create df with non-integer customer_id
    df = pd.DataFrame({
        'customer_id': [1, 'two', 3],
        'full_name': ['John Doe', 'Jane Doe', 'Jim Carter'],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'jim.carter@example.com'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
    })

    # ACT
    valid_df, rejected_df = validate_dataframe(
        df=df,
        required_columns=['customer_id', 'full_name', 'email', 'created_at'],
        non_null_columns=['customer_id', 'full_name', 'email', 'created_at'],
        integer_columns=['customer_id']
    )

    # ASSERT
    assert len(valid_df) == 2
    assert len(rejected_df) == 1
    assert 'integer' in rejected_df.iloc[0]['rejection_reason'].lower()

def test_validation_catches_duplicates():
    """Test that validation detects duplicates primary keys."""

    # ARRANGE - Create df with duplicate customer_id
    df = pd.DataFrame({
        'customer_id': [1, 2, 2],
        'full_name': ['John Doe', 'Jane Doe', 'Jane Doe'],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'jane.doe@example.com'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
    })

    # ACT
    valid_df, rejected_df = validate_dataframe(
        df=df,
        required_columns=['customer_id', 'full_name', 'email', 'created_at'],
        non_null_columns=['customer_id', 'full_name', 'email', 'created_at'],
        integer_columns=['customer_id']
    )

    # ASSERT
    assert len(valid_df) == 1
    assert len(rejected_df) == 2
    assert all('duplicate' in reason.lower() for reason in rejected_df['rejection_reason'])

def test_validation_all_rows_valid():
    """Test that validation passes when all rows are valid."""

    # ARRANGE - Create perfect df
    df = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'full_name': ['John Doe', 'Jane Doe', 'Jim Carter'],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'jim.carter@example.com'],
        'created_at': ['2024-01-15 10:30:00', '2024-01-16 14:22:00', '2024-01-17 09:15:00']
    })

    # ACT
    valid_df, rejected_df = validate_dataframe(
        df=df,
        required_columns=['customer_id', 'full_name', 'email', 'created_at'],
        non_null_columns=['customer_id', 'full_name', 'email', 'created_at'],
        integer_columns=['customer_id'],
        datetime_columns=['created_at']
    )

    # ASSERT
    assert len(valid_df) == 3
    assert len(rejected_df) == 0
