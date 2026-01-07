from __future__ import annotations

import uuid
from typing import ClassVar

from sqlmodel import DOUBLE, TEXT, TIMESTAMP, UUID, Field, SQLModel


class Ticker(SQLModel, table=True):
    """Ticker data model for cryptocurrency market data.

    Resides in: crypto_data.ticker
    """

    __tablename__: ClassVar[str] = "ticker"
    __table_args__: ClassVar[dict] = {"schema": "crypto_data"}

    id: UUID = Field(default_factory=uuid.uuid7, primary_key=True)
    exchange: TEXT
    dt: TIMESTAMP
    low: DOUBLE
    high: DOUBLE
    volume: DOUBLE
    buy: DOUBLE
    sell: DOUBLE
