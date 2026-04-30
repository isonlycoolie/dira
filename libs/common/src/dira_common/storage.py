from __future__ import annotations

import io
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

try:
    from google.cloud import storage as gcs_storage
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError:
    gcs_storage = None
    pa = None
    pq = None

if TYPE_CHECKING:
    import pandas as pd


class GCSParquetClient:
    def __init__(self, storage_client: Any | None = None, local_root: str | Path | None = None) -> None:
        self._logger = structlog.get_logger(__name__)
        self._storage_client = storage_client
        self._local_root = Path(local_root or ".gcs")
        if self._storage_client is None and gcs_storage is not None:
            self._storage_client = gcs_storage.Client()

    def _to_local_path(self, bucket_path: str) -> Path:
        relative_path = bucket_path.removeprefix("gs://") if bucket_path.startswith("gs://") else bucket_path
        return self._local_root / relative_path

    def _split_gs_uri(self, bucket_path: str) -> tuple[str, str]:
        relative_path = bucket_path.removeprefix("gs://")
        bucket_name, blob_name = relative_path.split("/", 1)
        return bucket_name, blob_name

    def write_parquet(self, bucket_path: str, df: "pd.DataFrame") -> None:
        row_count = len(df)
        byte_size = int(df.memory_usage(deep=True).sum())
        if self._storage_client is not None and pa is not None and pq is not None and bucket_path.startswith("gs://"):
            bucket_name, blob_name = self._split_gs_uri(bucket_path)
            buffer = io.BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            bucket = self._storage_client.bucket(bucket_name)
            bucket.blob(blob_name).upload_from_string(buffer.getvalue(), content_type="application/octet-stream")
        else:
            local_path = self._to_local_path(bucket_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_pickle(local_path)

        self._logger.info("wrote parquet", path=bucket_path, rows=row_count, bytes=byte_size)

    def read_parquet(self, bucket_path: str) -> "pd.DataFrame":
        import pandas as pd

        if self._storage_client is not None and pa is not None and pq is not None and bucket_path.startswith("gs://"):
            bucket_name, blob_name = self._split_gs_uri(bucket_path)
            bucket = self._storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            buffer = io.BytesIO(blob.download_as_bytes())
            return pq.read_table(buffer).to_pandas()

        return pd.read_pickle(self._to_local_path(bucket_path))
