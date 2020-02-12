from pyspark import SQLContext
from pyspark.sql import DataFrameReader
import os

DATABASE_URL = os.environ['MYSQL_URL']
properties = {'user': os.environ['MYSQL_USERNAME'],
              'password': os.environ['MYSQL_PASSWORD']}

def query_table(spark_obj, table,where='', limit=''):
    ''''Query Selcted table to pull all rows'''
    sqlctx = SQLContext(spark_obj)
    select_sql = ("(select * from {table} {where} {limit}) as x").format(table=table, where=where, limit=limit)
    df = DataFrameReader(sqlctx).jdbc(
        url='jdbc:%s' % DATABASE_URL,
        table=select_sql,
        properties=properties

    )
    return df


def all_tables(spark_obj):
    '''Pull all table names from Database'''
    sqlctx = SQLContext(spark_obj)
    df = DataFrameReader(sqlctx).jdbc(
        url='jdbc:%s' % DATABASE_URL,
        table='information_schema.tables',
        properties=properties
    )
    return df

