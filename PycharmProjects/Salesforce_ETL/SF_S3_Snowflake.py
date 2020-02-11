# from pyspark.sql import SparkSession
# from pyspark.sql.functions import unix_timestamp, from_unixtime, to_timestamp
# from pyspark.sql.types import *
from Spark_Main import Spark_object_builder
from S3_client import SimpleStorage
from Spark_S3_utils import *
from Transformations import *
from Spark_Snowflake_utils import *

def transformations(df, table):
    df = manager(df, table)
    return df


def main():
    bucket = 'rt-salesforce-data'
    database = ''
    schema = ''
    warehouse = ''
    keys = SimpleStorage(bucket).get_keys()
    spark = Spark_object_builder('S3', 'Snowflake','salesforce').return_obj()
    for key in keys:
        df = read_csv(spark, bucket, key)
        df = transformations(df, key.split('_')[0])
        write_table(database, schema, warehouse, key, df)




if __name__ == "__main__":
    main()








