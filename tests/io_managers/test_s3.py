"""Tests for S3IOManager."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from dagster import (
    AssetKey,
    InputContext,
    OutputContext,
    build_input_context,
    build_output_context,
)

from dagster_crypto_data.defs.io_managers import S3IOManager


@pytest.fixture
def s3_io_manager() -> S3IOManager:
    """Create S3IOManager instance."""
    return S3IOManager(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        region="us-east-1",
        use_ssl=False,
    )


@pytest.fixture
def sample_data() -> dict[str, Any]:
    """Sample data for testing."""
    return {
        "data": [
            [1735689600000, 50000.0, 51000.0, 49000.0, 50500.0, 100.5],
        ],
        "metadata": {"exchange": "binance", "symbol": "BTC/USDT"},
    }


@pytest.fixture
def output_context() -> OutputContext:
    """Create output context for testing."""
    return build_output_context(asset_key=AssetKey(["extract_binance_ohlcv"]))


@pytest.fixture
def input_context() -> InputContext:
    """Create input context for testing."""
    return build_input_context(asset_key=AssetKey(["extract_binance_ohlcv"]))


class TestS3IOManager:
    """Test S3IOManager functionality."""

    def test_get_s3_key(
        self,
        s3_io_manager: S3IOManager,
        output_context: OutputContext,
    ) -> None:
        """Test S3 key generation."""
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds
        s3_key = s3_io_manager._get_s3_key(output_context, timestamp)
        # asset_key is "extract_binance_ohlcv" -> "extract/binance/ohlcv_1704067200000.json"
        assert s3_key == "extract/binance/ohlcv_1704067200000.json"

    def test_get_s3_key_with_nested_asset(
        self,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test S3 key generation with nested asset key."""
        context = build_output_context(
            asset_key=AssetKey(["extract", "binance", "ohlcv"])
        )
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds
        s3_key = s3_io_manager._get_s3_key(context, timestamp)
        # Nested asset key ["extract", "binance", "ohlcv"] -> "extract/binance/ohlcv_1704067200000.json"
        assert s3_key == "extract/binance/ohlcv_1704067200000.json"

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    @patch("dagster_crypto_data.defs.io_managers.s3.S3IOManager._get_run_info")
    def test_handle_output_uploads_to_s3(
        self,
        mock_get_run_info: MagicMock,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
    ) -> None:
        """Test that handle_output uploads to S3."""
        # Mock run info
        mock_get_run_info.return_value = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        s3_io_manager.handle_output(output_context, sample_data)

        # Verify put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]

        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "extract/binance/ohlcv_1704067200000.json"
        assert call_kwargs["ContentType"] == "application/json"
        assert call_kwargs["Metadata"]["run_id"] == "test-run-123"
        assert call_kwargs["Metadata"]["loaded"] == "false"

        # Verify JSON content
        uploaded_data = json.loads(call_kwargs["Body"].decode("utf-8"))
        assert uploaded_data == sample_data

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    def test_handle_output_with_invalid_type_raises_error(
        self,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
        output_context: OutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-dict."""
        with pytest.raises(TypeError, match="S3IOManager expects dict"):
            s3_io_manager.handle_output(output_context, "not a dict")  # type: ignore

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    @patch("dagster_crypto_data.defs.io_managers.s3.S3IOManager._get_run_info")
    def test_handle_output_s3_error_raises(
        self,
        mock_get_run_info: MagicMock,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
    ) -> None:
        """Test that S3 errors are raised."""
        # Mock run info
        mock_get_run_info.return_value = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        mock_s3_client = MagicMock()
        mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )
        mock_boto3_client.return_value = mock_s3_client

        with pytest.raises(ClientError):
            s3_io_manager.handle_output(output_context, sample_data)

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    @patch("dagster_crypto_data.defs.io_managers.s3.S3IOManager._get_run_info")
    def test_load_input_downloads_from_s3(
        self,
        mock_get_run_info: MagicMock,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
        input_context: InputContext,
        sample_data: dict[str, Any],
    ) -> None:
        """Test that load_input downloads from S3."""
        # Mock run info
        mock_get_run_info.return_value = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        mock_s3_client = MagicMock()
        mock_response = {
            "Body": MagicMock(
                read=MagicMock(return_value=json.dumps(sample_data).encode("utf-8"))
            ),
            "ContentType": "application/json",
            "Metadata": {"loaded": "false"},
        }
        mock_s3_client.get_object.return_value = mock_response
        mock_boto3_client.return_value = mock_s3_client

        loaded_data = s3_io_manager.load_input(input_context)

        # Verify get_object was called
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="extract/binance/ohlcv_1704067200000.json",
        )

        # Verify copy_object was called to update metadata
        mock_s3_client.copy_object.assert_called_once()

        assert loaded_data == sample_data

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    @patch("dagster_crypto_data.defs.io_managers.s3.S3IOManager._get_run_info")
    def test_load_input_object_not_found_raises_error(
        self,
        mock_get_run_info: MagicMock,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
        input_context: InputContext,
    ) -> None:
        """Test that load_input raises FileNotFoundError if object doesn't exist."""
        # Mock run info
        mock_get_run_info.return_value = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        mock_s3_client = MagicMock()
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )
        mock_boto3_client.return_value = mock_s3_client

        with pytest.raises(FileNotFoundError, match="Asset not found in S3"):
            s3_io_manager.load_input(input_context)

    @patch("dagster_crypto_data.defs.io_managers.s3.boto3.client")
    def test_s3_client_configuration(
        self,
        mock_boto3_client: MagicMock,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test S3 client is configured correctly."""
        s3_io_manager._get_s3_client()

        mock_boto3_client.assert_called_once_with(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
            use_ssl=False,
        )

    def test_s3_io_manager_for_aws_s3(self) -> None:
        """Test S3IOManager configuration for AWS S3."""
        io_manager = S3IOManager(
            endpoint_url=None,  # AWS S3
            access_key="AWS_KEY",
            secret_key="AWS_SECRET",
            bucket="production-bucket",
            region="us-west-2",
            use_ssl=True,
        )

        assert io_manager.endpoint_url is None
        assert io_manager.region == "us-west-2"
        assert io_manager.use_ssl is True
