from pathlib import Path
import psycopg
from src.config import DBConfig

SCHEMA_PATH = Path("sql/schema.sql")

def create_tables() -> None:
    cfg = DBConfig()
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    with psycopg.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
    
    print("Tables created successfully or already exist.")

if __name__ == "__main__":
    create_tables()