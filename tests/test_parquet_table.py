import tempfile

import pyarrow.dataset
import pyarrow.parquet
from pyarrow import RecordBatch, schema
from pyarrow.fs import LocalFileSystem

from owlna.utils.arrow import cast_batch
from owlna.utils.metadata import dict_table_metadata_to_table
from tests import AthenaTestCase


class AthenaTableTests(AthenaTestCase):
    tempdir = tempfile.TemporaryDirectory()

    parquet_table = dict_table_metadata_to_table(
        AthenaTestCase.server.connect(),
        "AwsDataCatalog",
        "unittest",
        {'Name': 'pyathena_unittest', 'TableType': 'EXTERNAL_TABLE',
         'Columns': [{'Name': 'string', 'Type': 'string'}, {'Name': 'date', 'Type': 'date'},
                     {'Name': 'timestamp', 'Type': 'timestamp'}, {'Name': 'int', 'Type': 'int'},
                     {'Name': 'tinyint', 'Type': 'tinyint'}, {'Name': 'smallint', 'Type': 'smallint'},
                     {'Name': 'bigint', 'Type': 'bigint'}, {'Name': 'double', 'Type': 'double'},
                     {'Name': 'float', 'Type': 'float'}, {'Name': 'decimal', 'Type': 'decimal(38,18)'},
                     {'Name': 'char', 'Type': 'char(10)'}, {'Name': 'varchar', 'Type': 'varchar(120)'}],
         'PartitionKeys': [],
         'Parameters': {
             'EXTERNAL': 'TRUE',
             'inputformat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
             'location': "s3://" + tempdir.name + '/parquet_table',
             'outputformat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
             'serde.param.serialization.format': '1',
             'serde.serialization.lib': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
             'transient_lastDdlTime': '1667202766'}
         }
    )

    parquet_partition_table = dict_table_metadata_to_table(
        AthenaTestCase.server.connect(),
        "AwsDataCatalog",
        "unittest",
        {'Name': 'pyathena_unittest', 'TableType': 'EXTERNAL_TABLE',
         'Columns': [{'Name': 'string', 'Type': 'string'}, {'Name': 'date', 'Type': 'date'},
                     {'Name': 'timestamp', 'Type': 'timestamp'}, {'Name': 'int', 'Type': 'int'},
                     {'Name': 'tinyint', 'Type': 'tinyint'}, {'Name': 'smallint', 'Type': 'smallint'},
                     {'Name': 'bigint', 'Type': 'bigint'}, {'Name': 'double', 'Type': 'double'},
                     {'Name': 'float', 'Type': 'float'}, {'Name': 'decimal', 'Type': 'decimal(38,18)'},
                     {'Name': 'char', 'Type': 'char(10)'}, {'Name': 'varchar', 'Type': 'varchar(120)'}],
         'PartitionKeys': [
             {'Name': 'pstring', 'Type': 'string'},
             {'Name': 'pint', 'Type': 'int'}
         ],
         'Parameters': {
             'EXTERNAL': 'TRUE',
             'inputformat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
             'location': "s3://" + tempdir.name + '/parquet_partition_table',
             'outputformat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
             'serde.param.serialization.format': '1',
             'serde.serialization.lib': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
             'transient_lastDdlTime': '1667202766'}
         }
    )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_table_partitioned(self):
        self.assertEqual(False, self.parquet_table.partitioned)
        self.assertEqual(True, self.parquet_partition_table.partitioned)

    def test_table_location(self):
        self.assertEqual("s3://" + self.tempdir.name + "/parquet_table", self.parquet_table.location)
        self.assertEqual("s3://" + self.tempdir.name + '/parquet_partition_table', self.parquet_partition_table.location)

    def test_table_pyarrow_location(self):
        self.assertEqual(self.tempdir.name + "/parquet_table", self.parquet_table.pyarrow_location)
        self.assertEqual(self.tempdir.name + '/parquet_partition_table', self.parquet_partition_table.pyarrow_location)

    def test_table_file_format(self):
        self.assertEqual(pyarrow.dataset.ParquetFileFormat(), self.parquet_table.file_format)

    def test_table_file_format_from_classfication(self):
        self.parquet_table.parameters["classification"] = "parquet"
        self.assertEqual(pyarrow.dataset.ParquetFileFormat(), self.parquet_table.file_format)

    def test_table_partitioning_property_empty(self):
        expected = pyarrow.dataset.partitioning(
            schema=pyarrow.schema([]),
            flavor="hive"
        )

        self.assertEqual(expected.schema, self.parquet_table.partitioning.schema)

    def test_table_partitioning_property(self):
        expected = pyarrow.dataset.partitioning(
            schema=pyarrow.schema([
                pyarrow.field("pstring", pyarrow.string(), False),
                pyarrow.field("pint", pyarrow.int32(), False)
            ]),
            flavor="hive"
        )

        self.assertEqual(expected.schema, self.parquet_partition_table.partitioning.schema)

    def test_table_insert_batch_empty(self):
        self.parquet_table.insert_arrow_batch(RecordBatch.from_arrays(
            [
                [] for _ in self.parquet_table.schema_arrow
            ], schema=self.parquet_table.schema_arrow
        ))

    def test_table_insert_batch_full(self):
        data = RecordBatch.from_arrays(
            [
                pyarrow.array(["test", None])
            ],
            schema=pyarrow.schema(
                [
                    pyarrow.field("string", pyarrow.string())
                ]
            )
        )
        self.parquet_table.insert_arrow_batch(
            data,
            filesystem=LocalFileSystem(),
            existing_data_behavior="delete_matching"
        )

        self.assertEqual(
            cast_batch(data, self.parquet_table.schema_arrow),
            pyarrow.parquet.read_table(
                self.parquet_table.pyarrow_location,
                coerce_int96_timestamp_unit="ns"
            ).to_batches()[0]
        )

    def test_partition_table_insert_batch_full(self):
        athena_table = self.parquet_partition_table

        data = RecordBatch.from_arrays(
            [
                pyarrow.array([None, "pstring"]),
                pyarrow.array([None, 1]),
                pyarrow.array([None, "test"]),
                pyarrow.array([None, 10])
            ],
            schema=pyarrow.schema(
                [
                    pyarrow.field("pstring", pyarrow.string()),
                    pyarrow.field("pint", pyarrow.int64()),
                    pyarrow.field("string", pyarrow.string()),
                    pyarrow.field("int", pyarrow.int32())
                ]
            )
        )
        athena_table.insert_arrow_batch(
            data,
            filesystem=LocalFileSystem(),
            existing_data_behavior="delete_matching"
        )
        schema_arrow = schema(
            [
                *(field for field in athena_table.partitioning.schema),
                *(field for field in athena_table.schema_arrow)
            ],
            metadata=athena_table.schema_arrow.metadata
        )

        self.assertEqual(
            pyarrow.Table.from_batches([cast_batch(data, schema_arrow)]).select(["string", "int"]),
            pyarrow.parquet.read_table(athena_table.pyarrow_location).select(["string", "int"])
        )

    def test_partition_table_insert_batch_full_static_partition_values(self):
        athena_table = self.parquet_partition_table

        data = RecordBatch.from_arrays(
            [
                pyarrow.array([1, 1]),
                pyarrow.array(["test", "value"]),
                pyarrow.array([10, 20])
            ],
            schema=pyarrow.schema(
                [
                    pyarrow.field("pint", pyarrow.int64()),
                    pyarrow.field("string", pyarrow.string()),
                    pyarrow.field("int", pyarrow.int32())
                ]
            )
        )
        athena_table.insert_arrow_batch(
            data,
            base_dir={
                "pstring": "static_value"
            },
            filesystem=LocalFileSystem(),
            existing_data_behavior="delete_matching"
        )
        schema_arrow = schema(
            [
                pyarrow.field("pint", pyarrow.int64(), False),
                *(field for field in athena_table.schema_arrow)
            ],
            metadata=athena_table.schema_arrow.metadata
        )

        self.assertEqual(
            pyarrow.Table.from_batches([cast_batch(data, schema_arrow)]).select(["string", "int"]),
            pyarrow.parquet.read_table(athena_table.pyarrow_location).select(["string", "int"])
        )
