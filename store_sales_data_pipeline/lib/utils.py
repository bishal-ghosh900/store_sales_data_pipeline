from pyspark.sql import *
from pyspark.sql.functions import col, sum

from datetime import datetime

def load_data(spark: SparkSession, path: str):
    return spark.read \
        .option('header','true') \
        .option('inferSchema', 'true') \
        .csv(path)

def total_sales_per_product(df: DataFrame):
    return df.select(col('product'), (col('mrp') * col('sold_quantity')).alias('total_sales')) \
        .groupBy('product') \
        .agg(sum('total_sales').alias('total_sales')) \
        .sort('total_sales', ascending = False)

def total_sales_per_customer(df: DataFrame):
    return df.select(col('customer'), (col('mrp') * col('sold_quantity')).alias('total_sales')) \
        .groupBy('customer') \
        .agg(sum('total_sales').alias('total_sales')) \
        .sort('total_sales', ascending = False)

def total_sold_quantity_per_customer(df: DataFrame):
    return df.groupBy('customer') \
        .agg(sum('sold_quantity').alias('total_sold_quantity')) \
        .sort('total_sold_quantity', ascending = False) \
        
def load_matrices_in_hdfs(df: DataFrame, count: int):
    df.write \
        .option('header', 'true') \
        .csv(f'/output/project1/tf{count}/{datetime.today().strftime("%Y_%m_%d")}')