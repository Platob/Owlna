__all__ = ["Table"]

import os
from typing import Optional, Union

from pyarrow import Schema, RecordBatch, schema
from pyarrow.dataset import FileFormat, CsvFileFormat, ParquetFileFormat, write_dataset, \
    partitioning as partitioning_builder, Partitioning
from pyarrow.fs import S3FileSystem

from .config import DEFAULT_SAFE_MODE
from .utils.arrow import cast_batch


def parquet_ff():
    return ParquetFileFormat()


def csv_ff():
    return CsvFileFormat()


FILE_FORMATS = {
    **{
        k: parquet_ff for k in {
            "parquet",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        }
    },
    **{
        k: csv_ff for k in {
            "csv"
        }
    }
}


class Table:

    def __init__(
        self,
        connection: "owlna.connection.Connection",
        catalog: str,
        database: str,
        name: str,
        schema_arrow: Schema,
        partitioning: Partitioning,
        parameters: dict[str, str]
    ):
        self.connection = connection
        self.catalog = catalog
        self.database = database
        self.name = name
        self.schema_arrow = schema_arrow
        self.partitioning = partitioning
        self.parameters = parameters

        self._file_format = None

    def __repr__(self):
        return "AthenaTable('%s', '%s', '%s')" % (
            self.catalog, self.database, self.name
        )

    @property
    def location(self) -> str:
        return self.parameters["location"]

    @property
    def pyarrow_location(self) -> str:
        return self.location[5:]

    @property
    def s3fs(self) -> S3FileSystem:
        return self.connection.s3fs

    @property
    def file_format(self) -> FileFormat:
        if self._file_format is None:
            try:
                self._file_format = FILE_FORMATS[self.parameters["classification"]]()
            except KeyError:
                self._file_format = FILE_FORMATS[self.parameters["inputformat"]]()
        return self._file_format

    @property
    def partitioned(self):
        return len(self.partitioning.schema) > 0

    def insert_arrow_batch(
        self,
        batch: RecordBatch,
        base_dir: Union[str, dict] = "",
        basename_template: str = "",
        existing_data_behavior: str = "overwrite_or_ignore",
        safe: bool = DEFAULT_SAFE_MODE,
        filesystem: Optional[S3FileSystem] = None,
        format: Union[FileFormat, str] = None,
        file_options: Optional[dict] = None,
        **kwargs
    ) -> None:
        """
        See https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html#pyarrow.dataset.write_dataset

        :param batch: pyarrow.RecordBatch
        :param base_dir: str or dict to append to table.location path
        :param basename_template: file name template
            default with "part-{i}-%s.%s" % (os.urandom(12).hex(), self.file_format.default_extname)
        :param existing_data_behavior: like pyarrow.write_dataset
            'overwrite_or_ignore': default to append
            'error': raise error if exists
            'delete_matching': rm matching folder / partition folder, removes old data
        :param safe: bool for data cast to table.schema_arrow
            True/False; default owlna.config.DEFAULT_SAFE_MODE
        :param filesystem: default by current boto3.Session() credentials
        :param format: pyarrow.FileFormat
        :param file_options:
        :param kwargs: other pyarrow.write_dataset options
        """
        if not basename_template:
            basename_template = "part-{i}-%s.%s" % (os.urandom(12).hex(), self.file_format.default_extname)

        partitioning = self.partitioning

        if base_dir:
            if isinstance(base_dir, str):
                base_dir = {
                    kv[0]: kv[-1]
                    for kv in (_.split("=") for _ in base_dir.split("/") if "=" in _)
                }

            partitioning = partitioning_builder(
                schema=schema([
                    field for field in self.partitioning.schema if field.name not in base_dir
                ]),
                flavor="hive"
            )

            if base_dir:
                base_dir = self.pyarrow_location + "/%s" % "/".join((
                    '%s=%s' % (k, v)
                    for k, v in base_dir.items()
                ))
        else:
            base_dir = self.pyarrow_location

        if self.partitioned:
            schema_arrow = schema(
                [
                    *(field for field in partitioning.schema),
                    *(field for field in self.schema_arrow)
                ],
                metadata=self.schema_arrow.metadata
            )
        else:
            schema_arrow = self.schema_arrow

        if file_options:
            if isinstance(file_options, dict):
                file_options = self.file_format.make_write_options().update(**file_options)

        write_dataset(
            cast_batch(batch, schema_arrow, safe=safe),
            base_dir=base_dir,
            basename_template=basename_template,
            partitioning=partitioning,
            existing_data_behavior=existing_data_behavior,
            filesystem=filesystem if filesystem else self.s3fs,
            format=format if format else self.file_format,
            file_options=file_options,
            **kwargs
        )