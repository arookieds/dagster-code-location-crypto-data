from __future__ import annotations

from typing import TYPE_CHECKING

import ccxt
from dagster import AssetExecutionContext, AssetsDefinition, IOManager, Output, asset

if TYPE_CHECKING:
    from ccxt.base.exchange import Exchange


def extract_asset_factory(
    asset_name: str, group_name: str, exchange_id: str, io_manager: type[IOManager]
) -> AssetsDefinition:
    """
    Factory function to build a dagster asset, based on arguments.
    If possible, I would like to have pydantic validating the inputs to the function
    """

    @asset(name=f"{asset_name}", group_name=f"{group_name}", io_manager=io_manager)
    def build_asset(context: AssetExecutionContext):
        if exchange_id not in ccxt.exchanges:
            context.log.error(
                f"'{exchange_id}' is not a valid exchange id, supported by CCXT."
            )
            return {}
        else:
            context.log.debug(f"Getting {exchange_id} object")
            exchange: type[Exchange] = getattr(ccxt, exchange_id)

        raw = exchange.fetch_tickers()

        """
            build_asset returns an asset
        """
        return Output(value=raw)

    return build_asset
