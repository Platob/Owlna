__all__ = [
    "OwlnaBaseException",
    "OwlnaException",
    "CancelledQuery",
    "AthenaError"
]

from asyncio import CancelledError


class OwlnaBaseException(BaseException):
    pass


class OwlnaException(OwlnaBaseException):
    pass


class CancelledQuery(CancelledError, OwlnaBaseException):
    pass


class AthenaError(OwlnaException):

    def __init__(
        self, category: int, type: int, retryable: bool, message: str, full_message: str
    ):
        self.category = category
        self.type = type
        self.retryable = retryable
        self.message = message

        super().__init__(full_message)
