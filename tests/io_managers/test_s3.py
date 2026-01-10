"""Tests for S3IOManager."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import ClientError
from dagster import AssetKey

import dagster_crypto_data.defs.io_managers.s3 as s3_module
from dagster_crypto_data.defs.io_managers import S3IOManager


class FakeLog:
    """Fake logger for testing."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def debug(self, msg: str, **kwargs: Any) -> None:
        self.messages.append({"level": "debug", "msg": msg, **kwargs})

    def info(self, msg: str, **kwargs: Any) -> None:
        self.messages.append({"level": "info", "msg": msg, **kwargs})

    def warning(self, msg: str, **kwargs: Any) -> None:
        self.messages.append({"level": "warning", "msg": msg, **kwargs})

    def error(self, msg: str, **kwargs: Any) -> None:
        self.messages.append({"level": "error", "msg": msg, **kwargs})


class FakeOutputContext:
    """Fake output context for testing."""

    def __init__(self, asset_key: AssetKey) -> None:
        self.asset_key = asset_key
        self.log = FakeLog()


class FakeInputContext:
    """Fake input context for testing."""

    def __init__(self, asset_key: AssetKey) -> None:
        self.asset_key = asset_key
        self.log = FakeLog()
        self.upstream_output = None


class FakeS3Client:
    """Fake S3 client for testing."""

    def __init__(self) -> None:
        self.stored_objects: dict[str, dict[str, Any]] = {}
        self.put_object_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self.copy_object_calls: list[dict[str, Any]] = []
        self.head_object_calls: list[dict[str, Any]] = []
        self.raise_on_get: ClientError | None = None
        self.raise_on_put: ClientError | None = None

    def put_object(
        self,
        Bucket: str,
        Key: str,
        Body: bytes,
        Metadata: dict[str, str],
        ContentType: str,
    ) -> dict[str, Any]:
        """Store object in fake storage."""
        if self.raise_on_put:
            raise self.raise_on_put

        self.put_object_calls.append(
            {
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "Metadata": Metadata,
                "ContentType": ContentType,
            }
        )
        self.stored_objects[Key] = {
            "Body": Body,
            "Metadata": Metadata,
            "ContentType": ContentType,
        }
        return {}

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        """Get object from fake storage."""
        self.get_object_calls.append({"Bucket": Bucket, "Key": Key})

        if self.raise_on_get:
            raise self.raise_on_get

        if Key not in self.stored_objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "GetObject",
            )

        obj = self.stored_objects[Key]

        class FakeBody:
            def __init__(self, data: bytes) -> None:
                self._data = data

            def read(self) -> bytes:
                return self._data

        return {
            "Body": FakeBody(obj["Body"]),
            "ContentType": obj["ContentType"],
            "Metadata": obj["Metadata"].copy(),
        }

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        """Get object metadata from fake storage."""
        self.head_object_calls.append({"Bucket": Bucket, "Key": Key})

        if Key not in self.stored_objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
                "HeadObject",
            )

        return {"Metadata": self.stored_objects[Key]["Metadata"].copy()}

    def copy_object(
        self,
        Key: str,
        Bucket: str,
        CopySource: dict[str, str],
        Metadata: dict[str, str],
        MetadataDirective: str,
        ContentType: str,
    ) -> dict[str, Any]:
        """Copy object with new metadata."""
        self.copy_object_calls.append(
            {
                "Key": Key,
                "Bucket": Bucket,
                "CopySource": CopySource,
                "Metadata": Metadata,
                "MetadataDirective": MetadataDirective,
                "ContentType": ContentType,
            }
        )
        # Update metadata in place
        if Key in self.stored_objects:
            self.stored_objects[Key]["Metadata"] = Metadata.copy()
        return {}

    def get_paginator(self, operation: str) -> FakePaginator:
        """Return a fake paginator."""
        return FakePaginator(self.stored_objects)


class FakePaginator:
    """Fake paginator for list_objects_v2."""

    def __init__(self, stored_objects: dict[str, dict[str, Any]]) -> None:
        self.stored_objects = stored_objects

    def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
        """Return pages of objects matching prefix."""
        matching_keys = [k for k in self.stored_objects if k.startswith(Prefix)]
        if not matching_keys:
            return [{}]  # Empty page
        return [{"Contents": [{"Key": k} for k in matching_keys]}]


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
def fake_s3_client() -> FakeS3Client:
    """Create fake S3 client."""
    return FakeS3Client()


@pytest.fixture
def sample_data() -> dict[str, Any]:
    """Sample data for testing."""
    return {
        "data": {
            "BTC/USDT": {"symbol": "BTC/USDT", "last": 50000.0},
        },
        "metadata": {"exchange": "binance", "symbol": "BTC/USDT"},
    }


@pytest.fixture
def output_context() -> FakeOutputContext:
    """Create fake output context for testing."""
    return FakeOutputContext(asset_key=AssetKey(["extract_binance_ohlcv"]))


@pytest.fixture
def input_context() -> FakeInputContext:
    """Create fake input context for testing."""
    return FakeInputContext(asset_key=AssetKey(["extract_binance_ohlcv"]))


class TestS3IOManager:
    """Test S3IOManager functionality."""

    def test_get_s3_key(
        self,
        s3_io_manager: S3IOManager,
        output_context: FakeOutputContext,
    ) -> None:
        """Test S3 key generation."""
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds
        s3_key = s3_io_manager._get_s3_key(output_context, timestamp)  # type: ignore[arg-type]
        # asset_key is "extract_binance_ohlcv" -> "extract/binance/ohlcv_1704067200000.json"
        assert s3_key == "extract/binance/ohlcv_1704067200000.json"

    def test_get_s3_key_with_nested_asset(
        self,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test S3 key generation with nested asset key."""
        context = FakeOutputContext(asset_key=AssetKey(["extract", "binance", "ohlcv"]))
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds
        s3_key = s3_io_manager._get_s3_key(context, timestamp)  # type: ignore[arg-type]
        # Nested asset key ["extract", "binance", "ohlcv"] -> "extract/binance/ohlcv_1704067200000.json"
        assert s3_key == "extract/binance/ohlcv_1704067200000.json"

    def test_handle_output_uploads_to_s3(
        self,
        s3_io_manager: S3IOManager,
        output_context: FakeOutputContext,
        sample_data: dict[str, Any],
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that handle_output uploads to S3."""
        run_info = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_module, "get_run_info", lambda ctx: run_info)

        s3_io_manager.handle_output(output_context, sample_data)  # type: ignore[arg-type]

        # Verify put_object was called
        assert len(fake_s3_client.put_object_calls) == 1
        call = fake_s3_client.put_object_calls[0]

        assert call["Bucket"] == "test-bucket"
        assert call["Key"] == "extract/binance/ohlcv_1704067200000.json"
        assert call["ContentType"] == "application/json"
        assert call["Metadata"]["run_id"] == "test-run-123"
        assert call["Metadata"]["loaded"] == "false"

        # Verify JSON content
        uploaded_data = json.loads(call["Body"].decode("utf-8"))
        assert uploaded_data == sample_data

    def test_handle_output_with_invalid_type_raises_error(
        self,
        s3_io_manager: S3IOManager,
        output_context: FakeOutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-dict."""
        with pytest.raises(TypeError, match="S3IOManager expects dict"):
            s3_io_manager.handle_output(output_context, "not a dict")  # type: ignore[arg-type]

    def test_handle_output_s3_error_raises(
        self,
        s3_io_manager: S3IOManager,
        output_context: FakeOutputContext,
        sample_data: dict[str, Any],
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 errors are raised."""
        run_info = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        fake_s3_client.raise_on_put = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject",
        )

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_module, "get_run_info", lambda ctx: run_info)

        with pytest.raises(ClientError):
            s3_io_manager.handle_output(output_context, sample_data)  # type: ignore[arg-type]

    def test_load_input_downloads_from_s3_with_upstream(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        sample_data: dict[str, Any],
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input downloads from S3 when running with extract."""
        run_info = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        # Pre-populate fake S3 with the expected file
        s3_key = "extract/binance/ohlcv_1704067200000.json"
        fake_s3_client.stored_objects[s3_key] = {
            "Body": json.dumps(sample_data).encode("utf-8"),
            "ContentType": "application/json",
            "Metadata": {"loaded": "false", "run_id": "test-run-123"},
        }

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_module, "get_run_info", lambda ctx: run_info)
        # Simulate running with extract (has upstream output)
        monkeypatch.setattr(s3_io_manager, "_has_upstream_output", lambda ctx: True)

        loaded_data = s3_io_manager.load_input(input_context)  # type: ignore[arg-type]

        # Verify get_object was called
        assert len(fake_s3_client.get_object_calls) == 1
        assert fake_s3_client.get_object_calls[0]["Key"] == s3_key

        # Verify copy_object was called to update metadata
        assert len(fake_s3_client.copy_object_calls) == 1
        assert fake_s3_client.copy_object_calls[0]["Metadata"]["loaded"] == "true"

        # Verify returned data
        # Data is now returned as a list of records
        assert isinstance(loaded_data["data"], list)
        assert len(loaded_data["data"]) == 1
        # The record should contain the ticker data plus metadata
        record = loaded_data["data"][0]
        assert record["symbol"] == "BTC/USDT"
        assert record["last"] == 50000.0
        assert loaded_data["metadata"] == sample_data["metadata"]

    def test_load_input_standalone_loads_unloaded_files(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input loads all unloaded files when running standalone."""
        # Pre-populate fake S3 with multiple unloaded files
        file1_data = {
            "data": {"BTC/USDT": {"symbol": "BTC/USDT", "last": 50000}},
            "metadata": {"exchange": "binance"},
        }
        file2_data = {
            "data": {"ETH/USDT": {"symbol": "ETH/USDT", "last": 3000}},
            "metadata": {"exchange": "binance"},
        }

        fake_s3_client.stored_objects["extract/binance/ohlcv_1704067200000.json"] = {
            "Body": json.dumps(file1_data).encode("utf-8"),
            "ContentType": "application/json",
            "Metadata": {"loaded": "false"},
        }
        fake_s3_client.stored_objects["extract/binance/ohlcv_1704067300000.json"] = {
            "Body": json.dumps(file2_data).encode("utf-8"),
            "ContentType": "application/json",
            "Metadata": {"loaded": "false"},
        }
        # This file should be skipped (already loaded)
        fake_s3_client.stored_objects["extract/binance/ohlcv_1704067100000.json"] = {
            "Body": json.dumps({"data": {"OLD": {}}, "metadata": {}}).encode("utf-8"),
            "ContentType": "application/json",
            "Metadata": {"loaded": "true"},
        }

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        # Simulate running standalone (no upstream output)
        monkeypatch.setattr(s3_io_manager, "_has_upstream_output", lambda ctx: False)
        # Mock _list_unloaded_files to return our keys
        monkeypatch.setattr(
            s3_io_manager,
            "_list_unloaded_files",
            lambda ctx, prefix: [
                "extract/binance/ohlcv_1704067200000.json",
                "extract/binance/ohlcv_1704067300000.json",
            ],
        )
        # Mock _get_upstream_asset_key to return the context asset key
        monkeypatch.setattr(
            s3_io_manager, "_get_upstream_asset_key", lambda ctx: ctx.asset_key
        )

        loaded_data = s3_io_manager.load_input(input_context)  # type: ignore[arg-type]

        # Verify only unloaded files were fetched (2 files)
        assert len(fake_s3_client.get_object_calls) == 2

        # Verify data was merged correctly as a list of records
        assert isinstance(loaded_data["data"], list)
        assert len(loaded_data["data"]) == 2

        # Check that we have both records
        symbols = [r["symbol"] for r in loaded_data["data"]]
        assert "BTC/USDT" in symbols
        assert "ETH/USDT" in symbols
        assert "OLD" not in symbols

        # Verify both files were marked as loaded
        assert len(fake_s3_client.copy_object_calls) == 2

    def test_load_input_standalone_no_unloaded_files(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input returns empty data when no unloaded files exist."""
        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_io_manager, "_has_upstream_output", lambda ctx: False)

        loaded_data = s3_io_manager.load_input(input_context)  # type: ignore[arg-type]

        # Should return empty data structure (data is list)
        assert loaded_data == {"metadata": {}, "data": []}
        assert len(fake_s3_client.get_object_calls) == 0

    def test_load_input_object_not_found_with_upstream_skips(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input skips missing files and continues."""
        run_info = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        # Don't pre-populate - file doesn't exist
        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_module, "get_run_info", lambda ctx: run_info)
        monkeypatch.setattr(s3_io_manager, "_has_upstream_output", lambda ctx: True)

        # Should not raise, just return empty merged data
        loaded_data = s3_io_manager.load_input(input_context)  # type: ignore[arg-type]

        # Verify get_object was attempted
        assert len(fake_s3_client.get_object_calls) == 1

        # Verify empty result with correct structure (data is list)
        assert loaded_data == {"metadata": {}, "data": []}

    def test_load_input_s3_error_raises(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that non-NoSuchKey S3 errors are raised."""
        run_info = {
            "run_id": "test-run-123",
            "timestamp": "1704067200000",
            "dt": "24-01-01 00:00:00.000000",
        }

        fake_s3_client.raise_on_get = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "GetObject",
        )

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)
        monkeypatch.setattr(s3_module, "get_run_info", lambda ctx: run_info)
        monkeypatch.setattr(s3_io_manager, "_has_upstream_output", lambda ctx: True)

        with pytest.raises(ClientError):
            s3_io_manager.load_input(input_context)  # type: ignore[arg-type]

    def test_s3_client_configuration(
        self,
        s3_io_manager: S3IOManager,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test S3 client is configured correctly."""
        captured_kwargs: dict[str, Any] = {}

        def fake_boto_client(service: str, **kwargs: Any) -> FakeS3Client:
            captured_kwargs.update(kwargs)
            return FakeS3Client()

        import dagster_crypto_data.defs.io_managers.s3 as s3_module

        monkeypatch.setattr(s3_module.boto3, "client", fake_boto_client)

        s3_io_manager._get_s3_client()

        assert captured_kwargs["endpoint_url"] == "http://localhost:9000"
        assert captured_kwargs["aws_access_key_id"] == "minioadmin"
        assert captured_kwargs["aws_secret_access_key"] == "minioadmin"
        assert captured_kwargs["region_name"] == "us-east-1"
        assert captured_kwargs["use_ssl"] is False

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

    def test_has_upstream_output_returns_true_with_valid_upstream(
        self,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test _has_upstream_output returns True when upstream has run_id."""

        class FakeUpstream:
            run_id = "test-run-123"

        class FakeContext:
            upstream_output = FakeUpstream()

        result = s3_io_manager._has_upstream_output(FakeContext())  # type: ignore[arg-type]
        assert result is True

    def test_has_upstream_output_returns_false_without_upstream(
        self,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test _has_upstream_output returns False when no upstream."""

        class FakeContext:
            upstream_output = None

        result = s3_io_manager._has_upstream_output(FakeContext())  # type: ignore[arg-type]
        assert result is False

    def test_has_upstream_output_returns_false_when_run_id_none(
        self,
        s3_io_manager: S3IOManager,
    ) -> None:
        """Test _has_upstream_output returns False when run_id is None."""

        class FakeUpstream:
            run_id = None

        class FakeContext:
            upstream_output = FakeUpstream()

        result = s3_io_manager._has_upstream_output(FakeContext())  # type: ignore[arg-type]
        assert result is False

    def test_list_unloaded_files(
        self,
        s3_io_manager: S3IOManager,
        input_context: FakeInputContext,
        fake_s3_client: FakeS3Client,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test _list_unloaded_files returns only files with loaded=false."""
        # Pre-populate with mix of loaded and unloaded
        fake_s3_client.stored_objects["extract/binance/ohlcv_1.json"] = {
            "Body": b"{}",
            "ContentType": "application/json",
            "Metadata": {"loaded": "false"},
        }
        fake_s3_client.stored_objects["extract/binance/ohlcv_2.json"] = {
            "Body": b"{}",
            "ContentType": "application/json",
            "Metadata": {"loaded": "true"},
        }
        fake_s3_client.stored_objects["extract/binance/ohlcv_3.json"] = {
            "Body": b"{}",
            "ContentType": "application/json",
            "Metadata": {"loaded": "false"},
        }

        monkeypatch.setattr(s3_io_manager, "_get_s3_client", lambda: fake_s3_client)

        unloaded = s3_io_manager._list_unloaded_files(
            input_context,  # type: ignore[arg-type]
            "extract/binance/ohlcv",
        )

        assert len(unloaded) == 2
        assert "extract/binance/ohlcv_1.json" in unloaded
        assert "extract/binance/ohlcv_3.json" in unloaded
        assert "extract/binance/ohlcv_2.json" not in unloaded
