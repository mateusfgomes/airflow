from pyspark.sql import SparkSession

def enriching_data():
    print("Enriching data...")
    spark = SparkSession.builder.appName("EnrichingData").master("local[1]").getOrCreate()
    df_parquet = spark.read.parquet("/tmp/silver/car_accidents_casted")

    df_parquet = df_parquet.withColumn("percentual_fatalidades", (df_parquet.mortos/df_parquet.pessoas) * 100)
    
    output_path = "/tmp/silver/car_accidents_enriched"

    print("Writing into temporary file...")

    df_parquet.write \
        .mode("overwrite") \
        .partitionBy("ano") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print("Data enriched!")

    spark.stop()

if __name__ == "__main__":
    enriching_data()