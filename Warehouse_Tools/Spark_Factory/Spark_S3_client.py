import os

class Spark_S3_config(object):
    def __init__(self, spark_obj):
        spark = spark_obj
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ.get('ACCESS_KEY'))
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ.get('SECRET_KEY'))
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                             "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        self.spark = spark

    def return_obj(self):
        return self.spark




