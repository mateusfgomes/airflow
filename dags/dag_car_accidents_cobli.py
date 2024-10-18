from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from scripts import load_data, send_to_bronze, filter_null, casting_fields, enriching_data, send_to_silver

with DAG('vehicle_accidents_cobli') as dag:
    
    task_file_sensor = FileSensor(
        task_id='task_file_sensor',
        fs_conn_id='file_conn',
        filepath='acidentes_brasil.csv',
        poke_interval=10
    )

    task_load_data = PythonOperator(
        task_id='task_load_data',
        python_callable=load_data
    )

    task_send_to_bronze = PythonOperator(
        task_id='task_send_to_bronze',
        python_callable=send_to_bronze
    )

    task_filter_null = PythonOperator(
        task_id='task_filter_null',
        python_callable=filter_null
    )

    task_casting_fields = PythonOperator(
        task_id='task_casting_fields',
        python_callable=casting_fields
    )

    task_enriching_data = PythonOperator(
        task_id='task_enriching_data',
        python_callable=enriching_data
    )

    task_send_to_silver = PythonOperator(
        task_id='task_send_to_silver',
        python_callable=send_to_silver
    )
    
    task_file_sensor >> task_load_data >> task_send_to_bronze >> task_filter_null >> task_casting_fields >> \
    task_enriching_data >> task_send_to_silver
    