import io
from datetime import timedelta
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from src.config import MinIOConfig


class StorageClient:
    def __init__(self, cfg: MinIOConfig) -> None:
        parsed = urlparse(cfg.endpoint)
        host = parsed.netloc or parsed.path
        secure = parsed.scheme == "https"
        self._client = Minio(host, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=secure)
        self._bucket = cfg.bucket

    def ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def upload_object(self, key: str, data: bytes, content_type: str) -> None:
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def presigned_get_url(self, key: str, expires: timedelta = timedelta(hours=1)) -> str:
        return self._client.presigned_get_object(self._bucket, key, expires=expires)

    def delete_object(self, key: str) -> None:
        try:
            self._client.remove_object(self._bucket, key)
        except S3Error:
            pass
