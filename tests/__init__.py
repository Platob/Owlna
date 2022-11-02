import unittest

import boto3

from owlna.server import Athena


class AthenaTestCase(unittest.TestCase):
    PYATHENA_UNITTEST = "PYATHENA_UNITTEST"
    server = Athena(boto3.Session(profile_name="owlna", region_name="eu-west-1"))

    def create_test_table(self):
        sql = f"""CREATE EXTERNAL TABLE `PYATHENA_UNITTEST`(
  `string` string,
  `date` date,
  `timestamp` timestamp,
  `int` int,
  `tinyint` tinyint,
  `smallint` smallint,
  `bigint` bigint,
  `double` double,
  `float` float,
  `decimal` decimal(38,18),
  `char` char(10),
  `varchar` varchar(120)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://unittest/{self.PYATHENA_UNITTEST}'"""
