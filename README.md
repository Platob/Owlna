# Owlna
AWS Athena

### Begin

````python
import boto3
from owlna import Athena

with Athena(session=boto3.Session()).connect(
    query_options={"WorkGroup": "workgroup"}
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT * FROM "unittest"."pyathena_unittest" limit 10;""",
            wait=0.5 # set None to not wait end = async mode
        )
        cursor.wait(1) # ping every x second for query until completed
        # cursor.stop() # to stop
        print(cursor.get_query_execution())
        print(cursor.result, cursor.status)
        
        # Fetch data
        schema = cursor.schema_arrow
        # Generator[pyarrow.RecordBatch]
        for batch in cursor.fetch_arrow_batches():
            print(batch)
````

See /examples notebooks

### Utils

#### Tzdata

Install [IANA tzdata](https://arrow.apache.org/docs/cpp/build_system.html#download-timezone-database) to handle timezone with arrow

````python
def download_tzdata_windows(
    base_dir=None,
    year=2022,
    name="tzdata"
):
    import os
    import tarfile
    import urllib3

    http = urllib3.PoolManager()
    folder = base_dir if base_dir else os.path.join(os.path.expanduser('~'), "Downloads")
    tz_path = os.path.join(folder, "tzdata.tar.gz")
    
    with open(tz_path, "wb") as f:
        f.write(http.request('GET', f'https://data.iana.org/time-zones/releases/tzdata{year}f.tar.gz').data)
    
    folder = os.path.join(folder, name)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    tarfile.open(tz_path).extractall(folder)
    
    with open(os.path.join(folder, "windowsZones.xml"), "wb") as f:
        f.write(http.request('GET', f'https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml').data)

download_tzdata_windows(year=2022)
````