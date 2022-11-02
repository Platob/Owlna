__all__ = [
    "dict_table_metadata_to_table",
    "dict_to_pyarrow_field",
    "sqltype_to_datatype"
]

import pyarrow
from pyarrow import field, DataType
from pyarrow.dataset import partitioning

from owlna.table import Table


def fine_decimal(precision: int, scale: int):
    if precision > 38:
        return pyarrow.decimal256(precision, scale)
    else:
        return pyarrow.decimal128(precision, scale)


DATATYPES = {
    "string": lambda *args, **kwargs: pyarrow.string(),
    "date": lambda unit, tz, **kwargs: pyarrow.date32(),
    "timestamp": lambda unit, tz: pyarrow.timestamp(unit, tz),
    "int": lambda *args, **kwargs: pyarrow.int32(),
    "tinyint": lambda *args, **kwargs: pyarrow.int8(),
    "smallint": lambda *args, **kwargs: pyarrow.int16(),
    "bigint": lambda *args, **kwargs: pyarrow.int64(),
    "boolean": lambda *args, **kwargs: pyarrow.bool_(),
    "decimal": lambda precision, scale, **kwargs: fine_decimal(precision, scale),
    "double": lambda *args, **kwargs: pyarrow.float64(),
    "float": lambda *args, **kwargs: pyarrow.float32(),
    "binary": lambda n=-1, **kwargs: pyarrow.binary(n),
    "char": lambda *args, **kwargs: pyarrow.string(),
    "varchar": lambda *args, **kwargs: pyarrow.string()
}


def sqltype_to_datatype(sqltype: str) -> DataType:
    if '(' in sqltype:
        key, args = sqltype.split("(", 1)
        return DATATYPES[key](*(int(_) for _ in args[:-1].split(",")))
    else:
        return DATATYPES[sqltype](unit="us", tz="UTC")


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

