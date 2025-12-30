from ccxt.async_support.base.exchange import Exchange
import uuid
from sqlmodel import (
    Field,
    Session,
    SQLModel,
    UUID,
    TEXT,
    INTEGER,
    BOOLEAN,
    JSON,
    FLOAT,
    DOUBLE_PRECISION,
)


class DagsterConfig(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid.uuid7, primary_key=True)
    exchange: TEXT = Field(description="Name of the exchange")
    enabled: BOOLEAN = Field(description="Enable the exchange")
    config: JSON = Field(
        description="JSON configuration used by Dagster for extracting data for the exchange"
    )
