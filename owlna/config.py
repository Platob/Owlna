__all__ = [
    "DEFAULT_BOTO_CLIENT_CONFIG",
    "DEFAULT_SAFE_MODE"
]

import os

from botocore.config import Config

DEFAULT_BOTO_CLIENT_CONFIG = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)
DEFAULT_SAFE_MODE = os.environ.get("SAFE_MODE", "t")[0] in {"T", "t"}
