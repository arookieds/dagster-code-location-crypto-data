from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr, computed_field
from sqlalchemy import inspect
from sqlmodel import SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine


class DatabaseManagement(BaseModel):
    host: str
    port: int
    db_name: str
    username: str = Field(default_factory=str)
    password: str = Field(default_factory=str)
    db_type: str = Field(default="sqlite")
    _engine: Engine | None = PrivateAttr(default=None)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def url(self) -> str:
        if self.db_type not in ("sqlite", "duckdb"):
            return (
                f"{self.db_type}://{self.username}:{self.password}@"
                f"{self.host}:{self.port}/{self.db_name}"
            )
        return f"{self.db_type}:///{self.db_name}.db"

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.url)
            self.__create_schema_and_tables(self._engine)
        return self._engine

    def inspect_table_exists(self, model: type[SQLModel]) -> bool:
        """Checks if the table for a given SQLModel exists in the database."""
        inspector = inspect(self.engine)
        return bool(inspector.has_table(str(model.__tablename__)))

    def __create_schema_and_tables(self, engine: Engine) -> None:
        SQLModel.metadata.create_all(engine)
