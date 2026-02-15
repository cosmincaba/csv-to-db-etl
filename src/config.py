import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv() # loads variables from .env file into the environment

@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', 5432))
    name: str = os.getenv("DB_NAME", "etl_db")
    user: str = os.getenv("DB_USER", "etl_user")
    password: str = os.getenv("DB_PASSWORD", "etl_password")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"