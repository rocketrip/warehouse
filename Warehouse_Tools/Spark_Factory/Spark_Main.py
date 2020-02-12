from pyspark.sql import SparkSession
from Spark_S3_client import Spark_S3_config
from Spark_SnowFlake_client import Spark_Snowflake_config
from Spark_MySQL_client import Spark_MySQL_config
from Spark_S3_utils import *
from functools import reduce

class Spark_object_builder(object):
    def __init__(self, target, source, job):
        spark_obj = SparkSession.builder.appName(job).master("local[*]").getOrCreate()
        self.job = job
        self.source = source
        self.target = target
        self.transformation_map = {'S3':self.set_s3_config,
                                    'Snowflake':self.set_snowflake_config,
                                   'MySQL': self.set_MySQL_config}
        self.spark = self.configurations_manager(spark_obj)




    def configurations_manager(self, spark_obj):

        spark_obj_configured = reduce(
            lambda value, function: function(value),
            (
                self.transformation_map[self.source],
                self.transformation_map[self.target],
            ),
            spark_obj,
        )
        return spark_obj_configured


    def set_s3_config(self,spark_obj):
        spark_s3 = Spark_S3_config(spark_obj).return_obj()
        return spark_s3


    def set_snowflake_config(self,spark_obj):
        spark_sf = Spark_Snowflake_config(spark_obj).return_obj()
        return spark_sf

    def set_MySQL_config(self,spark_obj):
        spark_mysql = Spark_MySQL_config(spark_obj).return_obj()
        return spark_mysql


    def return_obj(self):
        return self.spark



def main():
    test = Spark_object_builder('S3', 'MySQL', 'test' ).return_obj()
    print('chill')
    # df = read_csv(test, 'rt-data-warehouse', 'Opportunity')
    # df.show()
    # print(test)
    pass

if __name__ == '__main__':
    main()