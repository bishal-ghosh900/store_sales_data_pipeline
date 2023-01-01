from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.models import Variable as var

from datetime import datetime, timedelta


def create_tf1():
    return f'''
        create external table if not exists total_sales_per_product(
            product string,
            total_sales double
        )
        row format delimited
        fields terminated by ','
        stored as textfile
        location '{var.get('output_folder_path')}/tf1/'
        tblproperties('skip.header.line.count'='1');
    '''

def create_tf2():
    return f'''
        create external table if not exists total_sales_per_customer(
            customer string,
            total_sales double
        )
        row format delimited
        fields terminated by ','
        stored as textfile
        location '{var.get('output_folder_path')}/tf2/'
        tblproperties('skip.header.line.count'='1');
    '''

def create_tf3():
    return f'''
        create external table if not exists total_sold_quantity_per_customer(
            customer string,
            total_sold_quantity double
        )
        row format delimited
        fields terminated by ','
        stored as textfile
        location '{var.get('output_folder_path')}/tf3/'
        tblproperties('skip.header.line.count'='1');
    '''

with DAG(
    'sales_report_analysis',
    start_date=datetime(2022,12,11),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    # this task will give an error if the input folder doesn't exit,
    # as if it can't find any file having today's date in its name
    # then it will throw an error
    t1 = SparkSubmitOperator(
        task_id = 'load_data_and_create_matices',
        conn_id='spark_conn',
        application_args=[var.get('file_path'), datetime.today().strftime('%Y_%m_%d')],
        application=var.get('spark_path'),
        conf=var.get('spark_configs_practice3', deserialize_json=True)
    )

    # it will create the transformation 1 -> total_sales_per_product
    t2 = HiveOperator(
        task_id = 'create_tf1',
        hive_cli_conn_id = 'hiveserver2_default',
        hql = create_tf1(),
    )

    # it will create the transformation 2 -> total_sales_per_customer
    t3 = HiveOperator(
        task_id = 'create_tf2',
        hive_cli_conn_id = 'hiveserver2_default',
        hql = create_tf2()
    )

    # it will create the transformation 3 -> total_sold_quantity_per_customer
    t4 = HiveOperator(
        task_id = 'create_tf3',
        hive_cli_conn_id = 'hiveserver2_default',
        hql = create_tf3()
    )

    t1 >> [t2, t3, t4]

    