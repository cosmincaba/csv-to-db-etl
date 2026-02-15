import psycopg
from src.config import DBConfig

def test_connection() -> None:
    cfg = DBConfig()
    print("Connecting to:", cfg.dsn)

    with psycopg.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select 1;")
            print("Connection successful, query result:", cur.fetchone())

if __name__ == "__main__":
    test_connection()