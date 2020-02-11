"""MySQL requires no configurations from the Spark Object, just the SQL Context"""

class Spark_MySQL_config(object):
    def __init__(self, spark_obj):
        self.spark = spark_obj


    def return_obj(self):
        return self.spark



