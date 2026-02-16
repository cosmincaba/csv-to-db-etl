import psycopg
from src.config import DBConfig

def check_tables() -> None:
    cfg = DBConfig()
    with psycopg.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select table_name
                from information_schema.tables
                where table_schema = 'public'
                order by table_name;
            """)    
            print(cur.fetchall())

if __name__ == "__main__":
    check_tables()