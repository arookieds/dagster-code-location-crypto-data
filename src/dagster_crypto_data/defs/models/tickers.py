import uuid

from sqlmodel import DOUBLE, TEXT, TIMESTAMP, UUID, Field, SQLModel


class Ticker(SQLModel, table=True):  # type: ignore[call-arg]
    id: UUID = Field(default_factory=uuid.uuid7, primary_key=True)
    exchange: TEXT
    dt: TIMESTAMP
    low: DOUBLE
    high: DOUBLE
    volume: DOUBLE
    buy: DOUBLE
    sell: DOUBLE
