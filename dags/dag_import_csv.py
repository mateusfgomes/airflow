from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def load_data():
    """Downloads data from S3 and saves it into raw_data variable"""

    # Create SparkSession
    spark = SparkSession.builder.appName('vehicle_accidents').getOrCreate()

    # Read CSV File
    df = spark.read.csv("..//inputdata//acidentes_brasil.csv")
    df.printSchema()
    

with DAG('vehicle_accidents_cobli', start_date=datetime(2024,10,16),
         schedule_interval='30 * * * *', catchup=False):
    
    task_load_data = PythonOperator(task_id='task_load_data', 
                               python_callable=load_data)