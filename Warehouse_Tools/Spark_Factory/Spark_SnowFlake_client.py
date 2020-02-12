import os

sfOptions = {
    "sfURL": os.environ.get('SF_ACCOUNT') + ".snowflakecomputing.com",
    "sfAccount": os.environ.get('SF_ACCOUNT'),
    "sfUser": os.environ.get('SF_USER'),
    "sfPassword": os.environ.get('SF_PASSWORD'),
    "sfDatabase": None,
    "sfSchema": None,
    "sfWarehouse": None,
}



class Spark_Snowflake_config(object):
    def __init__(self, spark_obj):
        spark = spark_obj
        spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(
                                                                                                spark._jvm.org.apache.spark.
                                                                                                sql.SparkSession.builder()
                                                                                                .getOrCreate())
        self.spark = spark

    def return_obj(self):
        return self.spark

