# store_sales_data_pipeline

* The data is going to be stored in hdfs every day.
* In airflow there is a SparkSubmitOperator, which is going to trigger a spark application
* At the time of reading, the spark application will search for those files which are going to have the current date at its file basename in the folder path hdfs:///input/project1. This path can be changed from the variables.json.
* There are three matrices being generated using spark. Those are -
    1. total sales per product
    2. total sales per customer
    3. total sold quantity per customer
* After the transformation all of the matrices are going to be stored in the hdfs.
