from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year


def load_data():
    """Downloads data from S3 and saves it into raw_data variable"""

    spark = SparkSession.builder.appName('vehicle_accidents').master("local[1]").getOrCreate()
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

    output_path = "/tmp/bronze/car_accidents"

    df.write \
        .mode("overwrite") \
        .partitionBy("ano", "UF") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    load_data()