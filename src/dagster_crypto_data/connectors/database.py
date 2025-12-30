from sqlmodel import Session, create_engine
from sqlalchemy import Engine
from pydantic import BaseModel, Field, computed_field


class DatabaseManagment(BaseModel):
    host: str
    port: int
    db_name: str
    username: str = Field(default_factory=str)
    password: str = Field(default_factory=str)
    db_type: str = Field(default="sqlite")
    _engine: Engine | None

    def __init__(
        self,
        host: str,
        port: int,
        db_name: str,
        username: str,
        password: str,
        db_type: str = "sqlite",
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.db_type = db_type

    def url(self):
        if self.db_type not in ("sqlite", "duckdb"):
            return f"{self.db_type}:///{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        else:
            return f"{self.db_type:///{self.db_name}.db}

    @property
    def engine(self):
        if not self._engine:
            self._engine = create_engine(self.url)
