import uuid

from sqlmodel import BOOLEAN, JSON, TEXT, UUID, Field, SQLModel


class DagsterConfig(SQLModel, table=True):  # type: ignore[call-arg]
    id: UUID | None = Field(default_factory=uuid.uuid7, primary_key=True)
    exchange: TEXT = Field(description="Name of the exchange")
    enabled: BOOLEAN = Field(description="Enable the exchange")
    config: JSON = Field(
        description="JSON configuration used by Dagster for extracting data for the exchange"
    )
