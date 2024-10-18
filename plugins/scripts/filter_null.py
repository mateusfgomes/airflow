from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def filter_null():
    spark = SparkSession.builder.appName("FilterNull").master("local[1]").getOrCreate()
    
    bronze_path = 'datalake/bronze/car_accidents'
    print("Loaded parquet path")
    
    df_parquet = spark.read.parquet(bronze_path)

    print("Loaded df in parquet mode")
    
    for column in df_parquet.columns:
        df_parquet = df_parquet.filter(col(column).isNotNull())

    print("Filtered null values successfully!")

    print("Fixing NA fields on classificacao_acidente column")
    df_parquet = df_parquet.withColumn("classificacao_acidente", when(col("classificacao_acidente") == "NA", 
         when(col("mortos") > 0, "Com Vitimas Fatais")
        .when((col("feridos_leves") + col("feridos_graves")) > 0, "Com Vitimas Feridas")
        .otherwise("Sem Vitimas"))
    .otherwise(col("classificacao_acidente")))

    print("Fixed NA values!")
    print("Now we have: " + str(df_parquet.filter(df_parquet.classificacao_acidente == "NA").count()) + " NAs")

    output_path = "/tmp/silver/car_accidents_filtered"

    df_parquet.write \
        .mode("overwrite") \
        .partitionBy("ano") \
        .parquet(output_path)

    print('::group::Filtered dataframe')
    print(df_parquet.show(5))
    print('::endgroup::')

    spark.stop()

if __name__ == "__main__":
    filter_null()

    