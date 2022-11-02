__all__ = [
    "DEFAULT_BOTO_CLIENT_CONFIG",
    "DEFAULT_SAFE_MODE",
    "QueryStates"
]

import os
from enum import Enum

from botocore.config import Config

DEFAULT_BOTO_CLIENT_CONFIG = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)
DEFAULT_SAFE_MODE = os.environ.get("SAFE_MODE", "t")[0] in {"T", "t"}


class QueryStates(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    DONE_STATES = {SUCCEEDED, FAILED, CANCELLED}
