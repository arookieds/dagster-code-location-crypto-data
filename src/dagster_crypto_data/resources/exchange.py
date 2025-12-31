from __future__ import annotations

from typing import TYPE_CHECKING

import ccxt
from dagster import ConfigurableResource
from pydantic import Field, field_validator

if TYPE_CHECKING:
    from ccxt.base.exchange import Exchange


class CCXTExchangeResource(ConfigurableResource):
    """Dagster resource for CCXT exchange clients."""

    exchange_id: str = Field(
        ...,
        description="CCXT exchange identifier (e.g., 'binance', 'bybit')",
    )

    @field_validator("exchange_id")
    @classmethod
    def validate_exchange_id(cls, v: str) -> str:
        """Validate exchange_id is supported by CCXT."""
        if v not in ccxt.exchanges:
            raise ValueError(f"'{v}' is not a valid CCXT exchange")
        return v

    def get_client(self) -> Exchange:
        """Get instantiated exchange client."""
        # Authenticated clients can be implemented here later
        return getattr(ccxt, self.exchange_id)()
