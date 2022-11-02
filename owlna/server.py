__all__ = ["Athena"]

from typing import Optional

from boto3 import Session
from botocore.config import Config
from pyarrow._s3fs import S3FileSystem

from .config import DEFAULT_BOTO_CLIENT_CONFIG
from .connection import Connection


class Athena:

    def __init__(
        self,
        session: Session = None
    ):
        self.session = session if session else Session()

    def connect(self, config: Config = DEFAULT_BOTO_CLIENT_CONFIG, query_options: Optional[dict] = None):
        return Connection(self, config=config, query_options=query_options)

    def cursor(self, config: Config = DEFAULT_BOTO_CLIENT_CONFIG):
        return self.connect(config).cursor()

    def pyarrow_s3filesystem(self, **kwargs):
        credentials = self.session.get_credentials()

        return S3FileSystem(
            secret_key=credentials.secret_key,
            access_key=credentials.access_key,
            region=self.session.region_name,
            session_token=credentials.token,
            **kwargs
        )
