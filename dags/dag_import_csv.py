import shutil
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year
# from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def load_data(**kwargs):
    """Downloads data from S3 and saves it into raw_data variable"""

    spark = SparkSession.builder.appName('vehicle_accidents').getOrCreate()
    print('Loading CSV file')

    df = spark.read.format('csv').options(delimiter=';', header='true', infer_schema='true') \
        .load('./inputdata/acidentes_brasil.csv')
    print('CSV file loaded!')
    print('::group::Loaded table')
    print(df.show(5))
    print('::endgroup::')

    print('::group::Schema')
    print(df.printSchema())
    print('::endgroup::')
    
    print('Creating column year to make partitioning easier')
    df = df.withColumn("data_inversa", to_date(col("data_inversa"), "dd/MM/yyyy"))
    df = df.withColumn("ano", year(col("data_inversa")))

    print('Column year created!')
    print('::group::Year dataframe')
    print(df.show(5))
    print('::endgroup::')

    output_path = "/tmp/car_accidents"

    df.write \
        .mode("overwrite") \
        .partitionBy("ano", "UF") \
        .parquet(output_path)

    kwargs['ti'].xcom_push(key='parquet_path', value=output_path)

# def send_to_s3(**kwargs):
#     ti = kwargs['ti']
#     output_path = ti.xcom_pull(task_ids='task_load_data', key='parquet_path')
    
#     s3_hook = S3Hook(aws_conn_id='s3')
#     bucket_name = 'car-accidents-airflow'
#     s3_key_prefix = 'bronze/car_accidents'

#     for root, dirs, files in os.walk(output_path):
#         for file in files:
#             local_file_path = os.path.join(root, file)
#             s3_key = os.path.join(s3_key_prefix, file)
#             s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=bucket_name)

def send_to_bronze(**kwargs):
    ti = kwargs['ti']
    output_path = ti.xcom_pull(task_ids='task_load_data', key='parquet_path')
    
    new_path = 'datalake/bronze'

    shutil.move(output_path, new_path)

def transform_data(**kwargs):
    spark = SparkSession.builder.appName("TransformData").getOrCreate()
    
    parquet_path = 'datalake/bronze/car_accidents'
    
    df = spark.read.parquet(parquet_path)
    
    df_transformado = df.filter(df.coluna1 > 1)

    df_transformado.show()



with DAG('vehicle_accidents_cobli', start_date=datetime(2024,10,16),
         schedule_interval='30 * * * *', catchup=False):
    
    task_load_data = PythonOperator(task_id='task_load_data', 
                                    python_callable=load_data)
    

    # task_send_to_s3 = PythonOperator(
    #     task_id="task_send_to_s3",
    #     python_callable=send_to_s3
    # )

    task_send_to_bronze = PythonOperator(
        task_id='task_send_to_bronze',
        python_callable=send_to_bronze
    )
    
    task_load_data >> task_send_to_bronze
    