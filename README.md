# CSV to Database ETL Pipeline

A Python-based ETL pipeline that loads raw CSV files into a PostgreSQL database with validation, transformation, and error handling.

## Prerequisites

- Docker Desktop
- Python 3.10+
- Git

## Getting Started

### 1. Install Python Dependencies

Create and activate a virtual environment, then install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Start PostgreSQL Database

This project uses **PostgreSQL running in Docker** to provide a reproducible local database environment.

From the project root:

```bash
docker compose up -d
```

### 3. Create Database Tables

```bash
python -m src.create_tables
```

## Usage

```bash
python -m src.main
```

The pipeline performs the following steps:
1. **Extract** - Read CSV files from the input directory
2. **Validate** - Check schema, nulls, and duplicates
3. **Transform** - Standardize types, dates, and formats
4. **Load** - Insert validated data into PostgreSQL

## Roadmap

- [x] Connect to database
- [ ] Extract .csv
- [ ] Validate (schema, nulls, duplicates)
- [ ] Transform (types, dates, standardization)
- [ ] Load into database
- [ ] Logging + run report
- [ ] Tests

## Troubleshooting

If you encounter connection issues, ensure Docker is running and PostgreSQL is accessible:

```bash
docker ps
```
