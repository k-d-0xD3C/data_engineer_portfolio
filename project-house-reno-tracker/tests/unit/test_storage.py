from unittest.mock import MagicMock, patch
from datetime import timedelta

import pytest

from src.storage import StorageClient
from src.config import MinIOConfig


@pytest.fixture
def cfg():
    return MinIOConfig(
        endpoint="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
    )


@pytest.fixture
def client(cfg):
    with patch("src.storage.Minio") as mock_minio_cls:
        mock_minio_cls.return_value = MagicMock()
        c = StorageClient(cfg)
        c._client = mock_minio_cls.return_value
        return c


def test_ensure_bucket_creates_when_missing(client):
    client._client.bucket_exists.return_value = False
    client.ensure_bucket()
    client._client.make_bucket.assert_called_once_with("test-bucket")


def test_ensure_bucket_skips_when_exists(client):
    client._client.bucket_exists.return_value = True
    client.ensure_bucket()
    client._client.make_bucket.assert_not_called()


def test_upload_object_calls_put(client):
    client.upload_object("reno/123/before/img.jpg", b"data", "image/jpeg")
    client._client.put_object.assert_called_once()
    call_kwargs = client._client.put_object.call_args
    assert call_kwargs[0][0] == "test-bucket"
    assert call_kwargs[0][1] == "reno/123/before/img.jpg"


def test_delete_object_calls_remove(client):
    client.delete_object("reno/123/before/img.jpg")
    client._client.remove_object.assert_called_once_with("test-bucket", "reno/123/before/img.jpg")


def test_presigned_url_delegates(client):
    client._client.presigned_get_object.return_value = "http://minio/presigned"
    url = client.presigned_get_url("some/key")
    assert url == "http://minio/presigned"
    client._client.presigned_get_object.assert_called_once_with(
        "test-bucket", "some/key", expires=timedelta(hours=1)
    )
