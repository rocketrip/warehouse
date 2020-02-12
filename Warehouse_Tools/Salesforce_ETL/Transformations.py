import pandas as pd
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import time


def manager(df, topic):
    for func_tup in transformation_map[topic]:
        udf_func = udf(func_tup[0], StringType())
        df = df.withColumn('', udf_func())
        df.show()
    return df

def transform_1():
    pass

def transform_2():
    pass

def transform_3():
    pass

def transform_4():
    pass

def transform_5():
    pass

def transform_6():
    pass

transformation_map = {'users': [transform_1] ,
                      'Account': [transform_2],
                      'Contract': [transform_3],
                      'Case': [transform_4],
                      'Opportunity': [transform_5, transform_6]}