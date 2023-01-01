# Store Sales Data Pipeline

* The data is going to be stored in hdfs every day.
* In airflow there is a SparkSubmitOperator, which is going to trigger a spark application
* At the time of reading, the spark application will search for those files which are going to have the current date at its file basename in the folder path hdfs:///input/project1. This path can be changed from the variables.json.
* There are three matrices being generated using spark. Those are -
    1. total sales per product
    2. total sales per customer
    3. total sold quantity per customer
* After the transformation creation, all of the matrices are going to be stored in the hdfs.
* Now using HiveOperator we need to create external tables which will point to the path in hdfs where the transformations are stored

Important notes:
* start the postgresql service before starting airflow webserver and scheduler
    1. sudo service postgresql start
    2. airflow scheduler
    3. airflow webserver
* start hadoop daemons but before that start ssh service
    1. sudo service ssh start
    2. start-all.sh
* start metadatabse service (for me it's mysql) and then start hive metastore service
    1. sudo service mysql start
    2. hive --service metastore
    3. check if commands are working in hive or not, if working then proceed, else solve it by yourself ha ha!  
* for further analysis on the result sets, we need to open hive shell and we need to set two properties
    1. hive> set mapred.input.dir.recursive=true;
    2. hive> set hive.mapred.supports.subdirectories=true;
    These properties are set for the external tables to catch not only all of the files available on their location path but also in the nested directories.
