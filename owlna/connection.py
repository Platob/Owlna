__all__ = ["Connection"]

from botocore.config import Config
from pyarrow.fs import S3FileSystem

from .config import DEFAULT_BOTO_CLIENT_CONFIG
from .cursor import Cursor
from .utils.metadata import dict_table_metadata_to_table


class Connection:

    def __init__(
        self,
        server: "Athena",
        config: Config = DEFAULT_BOTO_CLIENT_CONFIG
    ):
        self.server = server

        self.client = self.server.session.client("athena", config=config)
        self.s3fs = self.pyarrow_s3filesystem()
        self.closed = False

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            self.client.close()

    def cursor(self):
        return Cursor(self)

    # Table
    def table(
        self,
        catalog: str,
        database: str,
        name: str
    ):
        return dict_table_metadata_to_table(
            self, catalog, database,
            self.client.get_table_metadata(
                CatalogName=catalog,
                DatabaseName=database,
                TableName=name
            )["TableMetadata"]
        )

    def tables(
        self,
        catalog: str,
        database: str,
        page_size: int = 10,
        **kwargs
    ):
        for metas in self.client.get_paginator('list_table_metadata').paginate(
            CatalogName=catalog,
            DatabaseName=database,
            PaginationConfig={
                'PageSize': page_size,
                **kwargs
            }
        ):
            for meta in metas["TableMetadataList"]:
                yield dict_table_metadata_to_table(self, catalog, database, meta)

    def pyarrow_s3filesystem(self, **kwargs) -> S3FileSystem:
        # PyArrow 10
        # kwargs["retry_strategy"] = kwargs.get("retry_strategy", self.client._client_config.retries.get("total_max_attempts", 3))
        return self.server.pyarrow_s3filesystem(**kwargs)
