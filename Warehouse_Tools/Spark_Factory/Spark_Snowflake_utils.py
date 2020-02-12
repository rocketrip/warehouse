"""Snowflake Spark utility functions"""

from Spark_SnowFlake_client import sfOptions

def read_table(database, schema, Warehouse):
    '''Save for later'''
    pass


def update_table(database, schema, warehouse,table,df):
    df.write\
        .format("net.snowflake.spark.snowflake")\
        .option("sfURL", sfOptions["sfURL"]) \
        .option("sfUser", sfOptions["sfUser"]) \
        .option("sfPassword", sfOptions["sfPassword"]) \
        .option("sfDatabase", database) \
        .option("sfSchema",schema) \
        .option("sfWarehouse", warehouse) \
        .option("dbtable",table ).mode('append').save()



def write_table(database, schema, warehouse,table,df):
    df.write\
        .format("net.snowflake.spark.snowflake")\
        .option("sfURL", sfOptions["sfURL"]) \
        .option("sfUser", sfOptions["sfUser"]) \
        .option("sfPassword", sfOptions["sfPassword"]) \
        .option("sfDatabase", database) \
        .option("sfSchema",schema) \
        .option("sfWarehouse", warehouse) \
        .option("dbtable",table).save()

