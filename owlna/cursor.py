__all__ = ["Cursor"]


class Cursor:

    def __init__(self, connection: "Connection"):
        self.connection = connection

        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.closed = True
