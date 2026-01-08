"""Tests for CCXTExchangeResource."""

from __future__ import annotations

import ccxt
import pytest
from pydantic import ValidationError

from dagster_crypto_data.defs.resources.exchange import CCXTExchangeResource


class TestCCXTExchangeResource:
    """Test suite for CCXTExchangeResource."""

    def test_valid_resource_creation(self) -> None:
        """Test creating resource with valid exchange ID."""
        resource = CCXTExchangeResource(exchange_id="binance")
        assert resource.exchange_id == "binance"

    def test_invalid_exchange_id_raises_validation_error(self) -> None:
        """Test invalid exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id="invalid_exchange_that_does_not_exist")

    def test_multiple_valid_exchanges(self) -> None:
        """Test creating resources for multiple valid exchanges."""
        exchanges = ["binance", "bybit", "kraken", "coinbase", "okx", "bitfinex"]
        for exchange_id in exchanges:
            resource = CCXTExchangeResource(exchange_id=exchange_id)
            assert resource.exchange_id == exchange_id

    def test_get_client_returns_exchange_instance(self) -> None:
        """Test get_client returns an instantiated exchange."""
        resource = CCXTExchangeResource(exchange_id="binance")
        client = resource.get_client()

        # Verify it's an instance of the exchange class
        assert isinstance(client, ccxt.binance)
        assert hasattr(client, "fetch_tickers")
        assert hasattr(client, "fetch_ohlcv")

    def test_get_client_different_exchanges(self) -> None:
        """Test get_client works for different exchanges."""
        exchanges_and_classes = [
            ("binance", ccxt.binance),
            ("bybit", ccxt.bybit),
            ("kraken", ccxt.kraken),
        ]

        for exchange_id, expected_class in exchanges_and_classes:
            resource = CCXTExchangeResource(exchange_id=exchange_id)
            client = resource.get_client()
            assert isinstance(client, expected_class)

    def test_get_client_creates_new_instance_each_call(self) -> None:
        """Test get_client creates a new instance on each call."""
        resource = CCXTExchangeResource(exchange_id="binance")

        client1 = resource.get_client()
        client2 = resource.get_client()

        # Should be different instances
        assert client1 is not client2

    def test_get_client_instantiates_correct_exchange_class(self) -> None:
        """Test get_client properly instantiates the exchange class."""
        resource = CCXTExchangeResource(exchange_id="binance")
        client = resource.get_client()

        # Verify the client is the correct type and was instantiated
        assert isinstance(client, ccxt.binance)
        assert client.id == "binance"

    def test_resource_validation_case_sensitive(self) -> None:
        """Test exchange ID validation is case-sensitive."""
        # CCXT exchange IDs are lowercase
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id="Binance")

        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id="BINANCE")

    def test_resource_validation_with_whitespace(self) -> None:
        """Test exchange ID with whitespace raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id=" binance ")

        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id="binance ")

    def test_resource_validation_empty_string(self) -> None:
        """Test empty exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            CCXTExchangeResource(exchange_id="")

    def test_resource_field_description(self) -> None:
        """Test resource field has proper description."""
        # Access the field info
        field_info = CCXTExchangeResource.model_fields["exchange_id"]
        assert field_info.description is not None
        assert "CCXT" in field_info.description
        assert "exchange identifier" in field_info.description.lower()

    def test_resource_is_configurable_resource(self) -> None:
        """Test CCXTExchangeResource is a Dagster ConfigurableResource."""
        from dagster import ConfigurableResource

        resource = CCXTExchangeResource(exchange_id="binance")
        assert isinstance(resource, ConfigurableResource)

    def test_get_client_with_real_exchange(self) -> None:
        """Integration test: get_client with real CCXT exchange."""
        resource = CCXTExchangeResource(exchange_id="binance")
        client = resource.get_client()

        # Verify the client has expected CCXT exchange attributes
        assert hasattr(client, "id")
        assert client.id == "binance"
        assert hasattr(client, "name")
        assert hasattr(client, "countries")
        assert hasattr(client, "urls")
        assert hasattr(client, "api")

    def test_multiple_resources_independent(self) -> None:
        """Test multiple resource instances are independent."""
        resource1 = CCXTExchangeResource(exchange_id="binance")
        resource2 = CCXTExchangeResource(exchange_id="bybit")
        resource3 = CCXTExchangeResource(exchange_id="binance")

        assert resource1.exchange_id == "binance"
        assert resource2.exchange_id == "bybit"
        assert resource3.exchange_id == "binance"

        # Different instances even with same exchange_id
        assert resource1 is not resource3

    def test_resource_repr_contains_exchange_id(self) -> None:
        """Test resource repr contains exchange ID."""
        resource = CCXTExchangeResource(exchange_id="binance")
        repr_str = repr(resource)

        assert "binance" in repr_str
        assert "CCXTExchangeResource" in repr_str

    def test_get_client_exchange_has_required_methods(self) -> None:
        """Test client has all required CCXT exchange methods."""
        resource = CCXTExchangeResource(exchange_id="binance")
        client = resource.get_client()

        # Verify essential CCXT methods exist
        required_methods = [
            "fetch_tickers",
            "fetch_ticker",
            "fetch_ohlcv",
            "fetch_order_book",
            "fetch_trades",
            "fetch_balance",
            "fetch_markets",
        ]

        for method in required_methods:
            assert hasattr(client, method), f"Client missing method: {method}"
            assert callable(getattr(client, method))

    def test_resource_with_all_supported_exchanges(self) -> None:
        """Test resource creation with all CCXT supported exchanges."""
        # Test a representative sample of exchanges
        sample_exchanges = [
            "binance",
            "bybit",
            "kraken",
            "coinbase",
            "okx",
            "bitfinex",
            "huobi",
            "kucoin",
            "gateio",
            "bitget",
        ]

        for exchange_id in sample_exchanges:
            if exchange_id in ccxt.exchanges:
                resource = CCXTExchangeResource(exchange_id=exchange_id)
                assert resource.exchange_id == exchange_id

                # Verify client can be created
                client = resource.get_client()
                assert client is not None
