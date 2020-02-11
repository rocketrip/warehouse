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
    keys = SimpleStorage(bucket).get_keys()
    spark = Spark_object_builder('MySQL', 'S3','app_to_s3').return_obj()
    for key in keys:
        df = read_csv(spark, bucket, key)
        df = transformations(df, key.split('_')[0])





if __name__ == "__main__":
    main()
