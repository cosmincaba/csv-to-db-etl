import pandas as pd
import pytest
from src.transform import clean_column_name, transform_dataframe

def test_clean_column_name_spaces():
    """Test that spaces are converted to underscores."""

    # ARRANGE
    input_name = "Customer Name"
    
    # ACT
    result = clean_column_name(input_name)

    # ASSERT
    assert result == "customer_name"

def test_clean_column_name_mixed_case():
    """Test that mixed case is converted to lowercase."""

    assert clean_column_name("Customer_ID") == "customer_id"
    assert clean_column_name("EmailAddress") == "emailaddress"

def test_clean_column_name_special_characters():
    """Test that special characters are removed or converted."""

    assert clean_column_name("Customer-Name") == "customer_name"
    assert clean_column_name("Email@Address") == "email_address"
    assert clean_column_name("Full-Name!") == "full_name"

def test_clean_column_name_leading_trailing_underscores():
    """Test that leading/trailing underscores are removed."""

    assert clean_column_name("_customer_id_") == "customer_id"
    assert clean_column_name("__email__") == "email"

def test_transform_trims_whitespace():
    """Test that transformation trims whitespace from string columns."""

    # ARRANGE
    df = pd.DataFrame({
        'customer_id': [1, 2],
        'full_name': [' JOHN DOE ', ' jane doe '],
        'email': [' john.doe@example.com ', ' jane.doe@example.com '],
        'created_at': ['2024-01-01', '2024-01-02']
    })

    # ACT
    result = transform_dataframe(df, 'customers')

    # ASSERT
    assert result.iloc[0]['full_name'] == 'John Doe'
    assert result.iloc[0]['email'] == 'john.doe@example.com'
    assert result.iloc[1]['full_name'] == 'Jane Doe'
    assert result.iloc[1]['email'] == 'jane.doe@example.com'

def test_transform_customers_title_case():
    """Test that customer names are converted to title case."""

    # ARRANGE
    df = pd.DataFrame({
        'customer_id': [1, 2],
        'full_name': ['john doe', 'jane doe'],
        'email': ['john.doe@example.com', 'jane.doe@example.com'],
        'created_at': ['2024-01-01', '2024-01-02']
    })

    # ACT
    result = transform_dataframe(df, 'customers')

    # ASSERT
    assert result.iloc[0]['full_name'] == 'John Doe'
    assert result.iloc[1]['full_name'] == 'Jane Doe'

def test_transform_customers_lowercase_email():
    """Test that customer emails are converted to lowercase."""

    # ARRANGE
    df = pd.DataFrame({
        'customer_id': [1, 2],
        'full_name': ['John Doe', 'Jane Doe'],
        'email': ['JOHN.DOE@EXAMPLE.COM', 'JANE.DOE@EXAMPLE.COM'],
        'created_at': ['2024-01-01', '2024-01-02']
    })

    # ACT
    result = transform_dataframe(df, 'customers')

    # ASSERT
    assert result.iloc[0]['email'] == 'john.doe@example.com'
    assert result.iloc[1]['email'] == 'jane.doe@example.com'

def test_transform_parses_dates():
    """Test that datetime columns are parsed correctly."""

    # ARRANGE
    df = pd.DataFrame({
        'customer_id': [1, 2],
        'full_name': ['John Doe', 'Jane Doe'],
        'email': ['john.doe@example.com', 'jane.doe@example.com'],
        'created_at': ['2024-01-01', '2024-01-02 14:22:00']
    })

    # ACT
    result = transform_dataframe(df, 'customers')

    # ASSERT
    assert pd.api.types.is_datetime64_any_dtype(result['created_at'])
    assert result.iloc[0]['created_at'].year == 2024
    assert result.iloc[0]['created_at'].month == 1
    assert result.iloc[0]['created_at'].day == 1

    
