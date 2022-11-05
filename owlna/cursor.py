__all__ = ["Cursor"]

import time
from typing import Optional, Union, Iterable, Generator

import pyarrow.csv as pcsv
from pyarrow import schema, Schema, DataType, RecordBatch, RecordBatchReader
from pyarrow.fs import S3FileSystem

from owlna.config import QueryStates, DEFAULT_CURSOR_WAIT
from owlna.exception import AthenaError, CancelledQuery
from owlna.utils.metadata import query_result_column_to_pyarrow_field


class Cursor:

    def __init__(
        self,
        connection: "Connection"
    ):
        self.__result = None
        self.__status = None
        self.__statistics = None
        self.id = None
        self.closed = False
        self._schema_arrow = None

        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self):
        return "AthenaCursor(id='%s')" % self.id

    @property
    def client(self):
        return self.connection.client

    @property
    def s3fs(self) -> S3FileSystem:
        return self.connection.s3fs

    @property
    def done(self) -> bool:
        if self.__status is None:
            self.get_query_execution()
        return self.__status is not None

    @property
    def status(self) -> dict:
        if self.__status is None:
            return self.get_query_execution()["Status"]
        return self.__status

    @property
    def state(self) -> str:
        return self.status["State"]

    @property
    def statistics(self) -> dict:
        if self.__status is None:
            return self.get_query_execution()["Statistics"]
        return self.__status

    @property
    def result(self) -> dict:
        if self.__result is None:
            return self.get_query_execution()["ResultConfiguration"]
        return self.__result

    @property
    def schema_arrow(self) -> Schema:
        if self._schema_arrow is None:
            self.wait()
            self._schema_arrow = schema(
                [
                    query_result_column_to_pyarrow_field(_)
                    for _ in self.client.get_query_results(
                        QueryExecutionId=self.id, MaxResults=1
                    )["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                ]
            )
        return self._schema_arrow

    @property
    def output_location(self) -> str:
        return self.result["OutputLocation"]

    @property
    def query_options(self) -> dict:
        return self.connection.query_options

    def get_query_execution(self) -> dict:
        meta = self.client.get_query_execution(QueryExecutionId=self.id)["QueryExecution"]

        # persist
        if meta["Status"]["State"] in QueryStates.DONE_STATES.value:
            self.__status = meta["Status"]
            self.__statistics = meta["Statistics"]
            self.__result = meta["ResultConfiguration"]

        return meta

    def unpersist(self):
        self.__status = None
        self.__statistics = None
        self.__result = None
        self._schema_arrow = None

    def close(self):
        self.closed = True

    def execute(
        self,
        query: str,
        wait: Union[float, bool] = DEFAULT_CURSOR_WAIT,
        **kwargs
    ) -> "Cursor":
        """
        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.start_query_execution

        :param query:
        :param wait: wait query to be done with self.wait(tick=wait)
        :param kwargs: other boto3 kwargs
        """
        self.id = self.client.start_query_execution(
            QueryString=query,
            **{
                k: v for k, v in kwargs.items() if k not in self.query_options
            },
            **{
                k: v for k, v in self.query_options.items() if k not in kwargs
            }
        )["QueryExecutionId"]
        self.unpersist()

        if wait:
            self.wait(wait)

        return self

    def stop(self):
        if self.id:
            self.client.stop_query_execution(QueryExecutionId=self.id)

    def __await__(self):
        self.wait()

    def wait(self, tick: Union[float, bool] = DEFAULT_CURSOR_WAIT, raise_error: bool = True):
        if isinstance(tick, bool):
            tick = DEFAULT_CURSOR_WAIT
        try:
            while not self.done:
                time.sleep(tick)
        except BaseException as e:
            self.stop()
            raise e

        if raise_error:
            self.raise_exception()

    def raise_exception(self):
        state = self.state

        if state == QueryStates.CANCELLED.value:
            raise CancelledQuery("%s: Cancelled" % repr(self))
        elif state == QueryStates.FAILED.value:
            meta = self.status["AthenaError"]
            raise AthenaError(
                category=meta["ErrorCategory"],
                type=meta["ErrorType"],
                retryable=meta["Retryable"],
                message=meta["ErrorMessage"],
                full_message=self.status["StateChangeReason"]
            )

    def csv_column_types(
        self,
        include_columns: Iterable[str] = (),
        column_types: dict[str, DataType] = {}
    ):
        if include_columns:
            return {
                field.name: column_types.get(field.name, field.type)
                for field in self.schema_arrow
                if field.name in include_columns
            }
        return {
            field.name: column_types.get(field.name, field.type)
            for field in self.schema_arrow
        }

    # fetch
    def fetch_arrow_batches(
        self,
        block_size: int = 44040192,  # 42 Mb = 42 * 1024 **2
        include_columns: Iterable[str] = (),
        column_types: dict[str, DataType] = {},
        strings_can_be_null: bool = True,
        delimiter: str = ",",
        quote_char: str = '"',
        decimal_point: str = '.',
        compression: Optional[str] = None,
        **read_options
    ) -> Generator[RecordBatch, None, None]:
        column_types = self.csv_column_types(include_columns, column_types)

        with self.s3fs.open_input_stream(
            self.output_location[5:],
            compression=compression,
            buffer_size=block_size
        ) as stream:
            for batch in pcsv.open_csv(
                stream,
                read_options=pcsv.ReadOptions(
                    block_size=block_size,
                    **read_options
                ),
                parse_options=pcsv.ParseOptions(
                    delimiter=delimiter,
                    quote_char=quote_char
                ),
                convert_options=pcsv.ConvertOptions(
                    column_types=column_types,
                    strings_can_be_null=strings_can_be_null,
                    include_columns=include_columns,
                    decimal_point=decimal_point
                )
            ):
                yield batch

    def fetch_arrow(
        self,
        block_size: int = 44040192,  # 42 Mb = 42 * 1024 **2
        include_columns: Iterable[str] = (),
        column_types: dict[str, DataType] = {},
        strings_can_be_null: bool = True,
        delimiter: str = ",",
        quote_char: str = '"',
        decimal_point: str = '.',
        compression: Optional[str] = None,
        **read_options
    ):
        column_types = self.csv_column_types(include_columns, column_types)

        with self.s3fs.open_input_stream(
            self.output_location[5:],
            compression=compression,
            buffer_size=block_size
        ) as stream:
            return pcsv.read_csv(
                stream,
                read_options=pcsv.ReadOptions(
                    block_size=block_size,
                    **read_options
                ),
                parse_options=pcsv.ParseOptions(
                    delimiter=delimiter,
                    quote_char=quote_char
                ),
                convert_options=pcsv.ConvertOptions(
                    column_types=column_types,
                    strings_can_be_null=strings_can_be_null,
                    include_columns=include_columns,
                    decimal_point=decimal_point
                )
            )

    def reader(
        self,
        block_size: int = 44040192,  # 42 Mb = 42 * 1024 **2
        include_columns: Iterable[str] = (),
        column_types: dict[str, DataType] = {},
        strings_can_be_null: bool = True,
        delimiter: str = ",",
        quote_char: str = '"',
        decimal_point: str = '.',
        compression: Optional[str] = None,
        **read_options
    ):
        return RecordBatchReader.from_batches(
            self.schema_arrow,
            self.fetch_arrow_batches(
                block_size,
                include_columns,
                column_types,
                strings_can_be_null,
                delimiter,
                quote_char,
                decimal_point,
                compression,
                **read_options
            )
        )
