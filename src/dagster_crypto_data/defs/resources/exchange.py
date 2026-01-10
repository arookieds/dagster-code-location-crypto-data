from __future__ import annotations

from typing import TYPE_CHECKING

import ccxt
from dagster import ConfigurableResource

if TYPE_CHECKING:
    from ccxt.base.exchange import Exchange


class CCXTExchangeResource(ConfigurableResource):
    """Dagster resource for CCXT exchange clients."""

    @classmethod
    def validate_exchange_id(cls, v: str) -> str:
        """Validate exchange_id is supported by CCXT."""
        if v not in ccxt.exchanges:
            raise ValueError(f"'{v}' is not a valid CCXT exchange")
        return v

    def get_client(self, exchange_id: str) -> Exchange:
        """Get instantiated exchange client."""
        # Authenticated clients can be implemented here later
        _: str = CCXTExchangeResource.validate_exchange_id(exchange_id)
        return getattr(ccxt, exchange_id)()
