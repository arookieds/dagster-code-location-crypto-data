from abc import ABC
from typing import Any, ClassVar

from sqlmodel import SQLModel


class CryptoModel(SQLModel, ABC):
    """Abstract base class for all crypto data models.

    This class should not be used directly with table=True.
    Concrete subclasses should define their own __tablename__ and __table_args__.
    """

    __tablename__: ClassVar[str]
    __table_args__: ClassVar[dict[str, Any]]
