# CSV to Database ETL Pipeline

A production-ready Python ETL pipeline that extracts data from CSV files, validates quality, transforms for consistency, and loads into PostgreSQL with idempotent UPSERT logic.

## Features

- **Extract**: Read CSV files with automatic column name standardization
- **Validate**: 5-rule validation system with rejected row tracking
- **Transform**: Data cleaning and standardization (whitespace, casing, dates)
- **Load**: Idempotent UPSERT logic (safe to run multiple times)
- **Logging**: Structured logging to files with progress tracking
- **Config-Driven**: YAML-based configuration (no code changes for new tables)

## Prerequisites

- Docker Desktop
- Python 3.10+
- Git

## Quick Start

### 1. Install Dependencies
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\Activate.ps1

# Install requirements
pip install -r requirements.txt
```

### 2. Start PostgreSQL Database
```bash
docker compose up -d
```

### 3. Create Database Tables
```bash
python -m src.create_tables
```

### 4. Run the Pipeline
```bash
# Using config file (recommended)
python -m src.main --config configs/customers.yaml

# Or using command-line arguments (legacy)
python -m src.main --input data/raw/customers.csv --table customers
```

## Pipeline Architecture
```
CSV File → Extract → Validate → Transform → Load → PostgreSQL
                        ↓           ↓
                   Rejected    Cleaned CSV
                   Rows CSV    (for review)
```

### 4-Step Process

1. **Extract**: Read CSV and standardize column names to snake_case
2. **Validate**: Check data quality (required columns, nulls, types, duplicates)
3. **Transform**: Clean and standardize data (trim whitespace, normalize casing/dates)
4. **Load**: UPSERT to database (INSERT new rows, UPDATE existing rows)

## Configuration

The pipeline uses YAML configuration files for table-specific settings.

### Config File Structure
```yaml
# configs/customers.yaml
table: customers
input_path: data/raw/customers.csv
delimiter: ","

validation:
  required_columns:
    - customer_id
    - full_name
    - email
    - created_at
  non_null_columns:
    - customer_id
    - full_name
    - email
    - created_at
  integer_columns:
    - customer_id
  datetime_columns:
    - created_at

output:
  rejected_file: data/processed/rejected_customers.csv
  clean_file: data/processed/clean_customers.csv
```

### Adding New Tables

1. Create a new config file: `configs/your_table.yaml`
2. Define validation and transformation rules
3. Run: `python -m src.main --config configs/your_table.yaml`

**No code changes needed!**

## Project Structure
```
csv-to-db-etl/
├── configs/                    # YAML configuration files
│   ├── customers.yaml
│   └── transactions.yaml
├── data/
│   ├── raw/                    # Input CSV files
│   └── processed/              # Output files (rejected, cleaned)
├── logs/                       # Pipeline execution logs
├── sql/
│   └── schema.sql              # Database table definitions
├── src/
│   ├── config_loader.py        # Config file parser
│   ├── create_tables.py        # Database setup
│   ├── extract.py              # CSV extraction
│   ├── load.py                 # Database loading (UPSERT)
│   ├── logger.py               # Logging setup
│   ├── main.py                 # Pipeline orchestrator
│   ├── transform.py            # Data transformations
│   └── validate.py             # Data validation rules
├── docker-compose.yml          # PostgreSQL container
├── requirements.txt            # Python dependencies
└── README.md
```

## Validation Rules

The pipeline validates data quality with 5 rules:

1. **Required Columns**: All specified columns must exist
2. **Null Checks**: Critical fields cannot be empty
3. **Type Validation**: Fields must match expected types (integer, datetime)
4. **Format Validation**: Dates must be parseable
5. **Uniqueness**: Primary keys cannot have duplicates

Failed rows are saved to `data/processed/rejected_<table>.csv` with rejection reasons.

## Idempotency

The pipeline uses PostgreSQL's `ON CONFLICT DO UPDATE` for idempotent loads:

- **New rows** are INSERTed
- **Existing rows** (matched by primary key) are UPDATEd
- Safe to run multiple times without errors

### Example
```bash
# Run 1: Inserts 3 new rows
python -m src.main --config configs/customers.yaml
# Output: Inserted: 3, Updated: 0

# Run 2: Updates same 3 rows (no error!)
python -m src.main --config configs/customers.yaml
# Output: Inserted: 0, Updated: 3
```

## Output Files

After each run, the pipeline generates:

- **Logs**: `logs/etl_YYYYMMDD_HHMMSS.log` (detailed execution log)
- **Rejected Rows**: `data/processed/rejected_<table>.csv` (invalid data with reasons)
- **Cleaned Data**: `data/processed/clean_<table>.csv` (transformed data for review)

## Example Output
```
============================================================
Starting ETL Pipeline
============================================================
Input file: data/raw/customers.csv
Target table: customers

[1/4] Extracting data...
✓ Extracted 100 rows, 4 columns

[2/4] Validating data...
Validation Summary:
  Total rows: 100
  Valid rows: 95 (95.0%)
  Rejected rows: 5 (5.0%)

[3/4] Transforming data...
✓ Trimmed whitespace from all text columns
✓ Customer transformations complete

[4/4] Loading data to database...
✓ Upserted 95 rows into 'customers' table
  • Inserted: 23 new rows
  • Updated: 72 existing rows

============================================================
✅ Pipeline Complete!
   • Rows extracted: 100
   • Rows validated: 95
   • Rows rejected: 5
   • Rows transformed: 95
   • Rows loaded: 95
============================================================
```

## Database Schema
```sql
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    amount NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    transaction_date DATE NOT NULL
);
```

## Development Progress

- [x] Database connection and setup
- [x] Extract CSV files
- [x] Validate data quality (5 rules)
- [x] Transform and clean data
- [x] Load to database with UPSERT
- [x] Structured logging to files
- [x] CLI with argparse
- [x] Idempotent loads (run multiple times safely)
- [x] Config-driven pipeline (YAML)
- [ ] Unit tests (Day 10)
- [ ] Error handling improvements (Day 11)
- [ ] Run metrics and reports (Day 12)
- [ ] Multiple dataset support (Day 14)

## Troubleshooting

### Docker Issues
```bash
# Check if PostgreSQL is running
docker ps

# View container logs
docker logs csv_to_db_postgres

# Restart containers
docker compose restart
```

### Database Connection
```bash
# Connect to PostgreSQL
docker exec -it csv_to_db_postgres psql -U etl_user -d etl_db

# Check tables
\dt

# View data
SELECT * FROM customers LIMIT 10;

# Exit
\q
```

### Pipeline Issues
```bash
# Check recent logs
ls logs/
cat logs/etl_*.log

# View rejected rows
cat data/processed/rejected_customers.csv
```

## Technologies Used

- **Python 3.10+**: Core language
- **pandas**: Data manipulation and CSV processing
- **psycopg3**: PostgreSQL database adapter
- **PyYAML**: Configuration file parsing
- **Docker**: PostgreSQL containerization

## License

This project is open source and available for educational purposes.
