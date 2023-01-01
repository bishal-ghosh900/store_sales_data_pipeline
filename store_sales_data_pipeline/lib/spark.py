from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys
from utils import *
from logging import *

spark = SparkSession.builder.getOrCreate()

if len(sys.argv) < 3:
    error('Please provide valid number of arguments')
    sys.exit(-1)

file_path = f'{sys.argv[1]}/*_{sys.argv[2]}.csv'

print('Reading the data...')
df = load_data(spark, file_path)

# df.show()


print('Generating the matrices...')
total_sales_per_product = total_sales_per_product(df)
# total_sales_per_product.show()

total_sales_per_customer = total_sales_per_customer(df)
# total_sales_per_customer.show()

total_sold_quantity_per_customer = total_sold_quantity_per_customer(df)
# total_sold_quantity_per_customer.show()

print('Matrix generation completed')


print('Writing the result set of the matrices in the hdfs')
load_matrices_in_hdfs(total_sales_per_product, 1)
load_matrices_in_hdfs(total_sales_per_customer, 2)
load_matrices_in_hdfs(total_sold_quantity_per_customer, 3)
print('Write completed')

# df.show()

spark.stop()