__all__ = [
    "dict_table_metadata_to_table",
    "dict_to_pyarrow_field",
    "sqltype_to_datatype",
    "query_result_column_to_pyarrow_field"
]

from typing import Optional

import pyarrow
from pyarrow import field, DataType, Field
from pyarrow.dataset import partitioning

from owlna.table import Table
from owlna.utils.arrow import TIMETYPES


def fine_decimal(precision: int, scale: int):
    if precision > 38:
        return pyarrow.decimal256(precision, scale)
    else:
        return pyarrow.decimal128(precision, scale)


def int_to_timeunit(i: int) -> str:
    if i == 0:
        return "s"
    elif i <= 3:
        return "ms"
    elif i <= 6:
        return "us"
    return "ns"


DATATYPES = {
    "string": lambda precision=None, *args, **kwargs:
        pyarrow.large_string() if precision is not None and precision > 42000 else pyarrow.string(),
    "date": lambda unit, tz, **kwargs: pyarrow.date32(),
    "timestamp": lambda unit, tz, precision=None, **kwarg:
        pyarrow.timestamp(int_to_timeunit(precision), tz) if precision else pyarrow.timestamp(unit, tz),
    "int": lambda *args, **kwargs: pyarrow.int32(),
    "integer": lambda *args, **kwargs: pyarrow.int32(),
    "tinyint": lambda *args, **kwargs: pyarrow.int8(),
    "smallint": lambda *args, **kwargs: pyarrow.int16(),
    "bigint": lambda *args, **kwargs: pyarrow.int64(),
    "boolean": lambda *args, **kwargs: pyarrow.bool_(),
    "decimal": lambda precision, scale, **kwargs: fine_decimal(precision, scale),
    "double": lambda *args, **kwargs: pyarrow.float64(),
    "float": lambda *args, **kwargs: pyarrow.float32(),
    "binary": lambda precision=None, **kwargs:
        pyarrow.large_binary() if precision is not None and precision > 42000 else pyarrow.binary(),
    "char": lambda precision=None, *args, **kwargs:
        pyarrow.large_string() if precision is not None and precision > 42000 else pyarrow.string(),
    "varchar": lambda precision=None, *args, **kwargs:
        pyarrow.large_string() if precision is not None and precision > 42000 else pyarrow.string(),
    "time": lambda precision=9, *args, **kwargs: TIMETYPES[int_to_timeunit(precision)],
    "timestamp with time zone": lambda **kwargs: pyarrow.string()
}


def sqltype_to_datatype(
    sqltype: str,
    unit: str = "us",
    tz: Optional[str] = "UTC",
    **kwargs
) -> DataType:
    if '(' in sqltype:
        key, args = sqltype.split("(", 1)
        return DATATYPES[key](*(int(_) for _ in args[:-1].split(",")))
    else:
        return DATATYPES[sqltype](unit=unit, tz=tz, **kwargs)


def dict_to_pyarrow_field(meta: dict, nullable: bool = True):
    return field(
        meta["Name"],
        sqltype_to_datatype(meta["Type"]),
        nullable,
        {k: v for k, v in meta.items() if k != "Name"}
    )


def dict_table_metadata_to_table(
    connection: "owlna.connection.Connection",
    catalog: str,
    database: str,
    meta: dict
) -> Table:
    """
    Parse dict returned from boto3.client to owlna.Table

    :param connection: owlna.connection
    :param catalog: Athena catalog name
    :param database: Athena database name
    :param meta: dict returned by boto3.client.get_table_metadata
    :rtype Table: owlna.Table
    """

    return Table(
        connection=connection,
        catalog=catalog,
        database=database,
        name=meta["Name"],
        schema_arrow=pyarrow.schema(
            [dict_to_pyarrow_field(_, nullable=True) for _ in meta["Columns"]],
            metadata={
                "engine": "athena",
                "catalog": catalog,
                "database": database
            }
        ),
        partitioning=partitioning(
            schema=pyarrow.schema([dict_to_pyarrow_field(_, nullable=False) for _ in meta["PartitionKeys"]]),
            flavor="hive"
        ),
        parameters=meta["Parameters"]
    )


def query_result_column_to_pyarrow_field(meta: dict) -> Field:
    return field(
        meta["Name"],
        sqltype_to_datatype(
            meta["Type"], precision=meta["Precision"], scale=meta["Scale"], tz=None
        ),
        nullable=not meta["Nullable"].startswith("T"),  # = 'UNKNOWN' atm
        metadata={
            k: str(v) for k, v in meta.items() if k not in {"Name", "Nullable"}
        }
    )
