from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('vehicle_accidents_cobli', start_date=datetime(2024,10,16),
         schedule_interval='30 * * * *', catchup=False) as dag:
    
    task_load_data = SparkSubmitOperator(
        task_id='task_load_data', 
        application='scripts/load_data.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        master='local[*]',
        dag=dag
    )

    task_send_to_bronze = SparkSubmitOperator(
        task_id='task_send_to_bronze',
        application='scripts/send_to_bronze.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        dag=dag
    )

    task_filter_null = SparkSubmitOperator(
        task_id='task_filter_null',
        application='scripts/filter_null.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        dag=dag
    )

    task_casting_fields = SparkSubmitOperator(
        task_id='task_casting_fields',
        application='scripts/casting_fields.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        dag=dag
    )

    task_enriching_data = SparkSubmitOperator(
        task_id='task_enriching_data',
        application='scripts/enriching_data.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        dag=dag
    )

    task_send_to_silver = SparkSubmitOperator(
        task_id='task_send_to_silver',
        application='scripts/send_to_silver.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        dag=dag
    )
    
    task_load_data >> task_send_to_bronze >> task_filter_null >> task_casting_fields >> task_enriching_data >> task_send_to_silver
    