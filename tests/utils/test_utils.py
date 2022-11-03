import pyarrow
import pyarrow as pa

from owlna.utils.metadata import dict_to_pyarrow_field, query_result_column_to_pyarrow_field
from tests import AthenaTestCase


class MetadataUtilsTests(AthenaTestCase):

    def test_dict_to_pyarrow_field(self):
        raw = [
            {'Name': 'string', 'Type': 'string'},
            {'Name': 'date', 'Type': 'date'},
            {'Name': 'timestamp', 'Type': 'timestamp'},
            {'Name': 'int', 'Type': 'int'},
            {'Name': 'tinyint', 'Type': 'tinyint'},
            {'Name': 'smallint', 'Type': 'smallint'},
            {'Name': 'bigint', 'Type': 'bigint'},
            {'Name': 'double', 'Type': 'double'},
            {'Name': 'float', 'Type': 'float'},
            {'Name': 'decimal', 'Type': 'decimal(38,18)'},
            {'Name': 'char', 'Type': 'char(10)'},
            {'Name': 'varchar', 'Type': 'varchar(120)'},
            {'Name': 'binary', 'Type': 'binary'},
            {'Name': 'time', 'Type': 'time'},
            {'Name': 'timestamp with time zone', 'Type': 'timestamp with time zone'}
        ]

        expected = [
            pa.field("string", pa.string()),
            pa.field("date", pa.date32()),
            pa.field("timestamp", pa.timestamp("us", "UTC")),
            pa.field("int", pa.int32()),
            pa.field("tinyint", pa.int8()),
            pa.field("smallint", pa.int16()),
            pa.field("bigint", pa.int64()),
            pa.field("double", pa.float64()),
            pa.field("float", pa.float32()),
            pa.field("decimal", pa.decimal128(38, 18)),
            pa.field("char", pa.string()),
            pa.field("varchar", pa.string()),
            pa.field("binary", pa.binary(-1)),
            pa.field("time", pa.time64("ns")),
            pa.field("timestamp with time zone", pa.string())
        ]

        for _1, _2 in zip(raw, expected):
            self.assertEqual(dict_to_pyarrow_field(_1), _2)

    def test_dict_to_pyarrow_field_metadata(self):
        self.assertEqual(
            {b'Type': b'date'},
            dict_to_pyarrow_field({'Name': 'date', 'Type': 'date'}).metadata
        )
        self.assertEqual(
            {b'Type': b'date', b'Comment': b'test Comment'},
            dict_to_pyarrow_field({'Name': 'date', 'Type': 'date', 'Comment': 'test Comment'}).metadata
        )

    def test_query_result_column_to_pyarrow_field(self):
        self.assertEqual(
            pyarrow.field("string", pyarrow.large_string()),
            query_result_column_to_pyarrow_field({
                'CatalogName': 'hive',
                'SchemaName': '',
                'TableName': '',
                'Name': 'string',
                'Label': 'string',
                'Type': 'varchar',
                'Precision': 2147483647,
                'Scale': 0,
                'Nullable': 'UNKNOWN',
                'CaseSensitive': True
            })
        )
        self.assertEqual(
            pyarrow.field("string", pyarrow.string()),
            query_result_column_to_pyarrow_field({
                'CatalogName': 'hive',
                'SchemaName': '',
                'TableName': '',
                'Name': 'string',
                'Label': 'string',
                'Type': 'varchar',
                'Precision': 41999,
                'Scale': 0,
                'Nullable': 'UNKNOWN',
                'CaseSensitive': True
            })
        )
        self.assertEqual(
            pyarrow.field("timestamp", pyarrow.timestamp("ms")),
            query_result_column_to_pyarrow_field({
                'CatalogName': 'hive',
                'SchemaName': '',
                'TableName': '',
                'Name': 'timestamp',
                'Label': 'string',
                'Type': 'timestamp',
                'Precision': 3,
                'Scale': 0,
                'Nullable': 'UNKNOWN',
                'CaseSensitive': True
            })
        )
        self.assertEqual(
            pyarrow.field("timestamp with time zone", pyarrow.string()),
            query_result_column_to_pyarrow_field({
                'CatalogName': 'hive',
                'SchemaName': '',
                'TableName': '',
                'Name': 'timestamp with time zone',
                'Label': 'string',
                'Type': 'timestamp with time zone',
                'Precision': 3,
                'Scale': 0,
                'Nullable': 'UNKNOWN',
                'CaseSensitive': True
            })
        )

    def test_query_result_column_to_pyarrow_field_metadata(self):
        self.assertEqual(
            {b'CaseSensitive': b'True',
             b'CatalogName': b'hive',
             b'Label': b'string',
             b'Precision': b'2147483647',
             b'Scale': b'0',
             b'SchemaName': b'',
             b'TableName': b'',
             b'Type': b'varchar'},
            query_result_column_to_pyarrow_field({
                'CatalogName': 'hive',
                'SchemaName': '',
                'TableName': '',
                'Name': 'string',
                'Label': 'string',
                'Type': 'varchar',
                'Precision': 2147483647,
                'Scale': 0,
                'Nullable': 'UNKNOWN',
                'CaseSensitive': True
            }).metadata
        )
