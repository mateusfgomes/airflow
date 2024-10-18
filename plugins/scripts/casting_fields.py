from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, concat, lit
from pyspark.sql.types import *

def casting_fields():
    print("Casting fields...")
    spark = SparkSession.builder.appName("CastingFields").master("local[1]").getOrCreate()

    df_parquet = spark.read.parquet("/tmp/silver/car_accidents_filtered")

    df_parquet = df_parquet.withColumn("id",df_parquet.id.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("data_inversa", to_date(col("data_inversa"), "dd/MM/yyyy"))
    df_parquet = df_parquet.withColumn("data_horario", to_timestamp(concat(col("data_inversa"), lit(" "), col("horario")), "yyyy-MM-dd HH:mm:ss"))
    df_parquet = df_parquet.withColumn("br",df_parquet.br.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("km",df_parquet.km.cast(FloatType()))
    df_parquet = df_parquet.withColumn("pessoas",df_parquet.pessoas.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("mortos",df_parquet.mortos.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("feridos_leves",df_parquet.feridos_leves.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("feridos_graves",df_parquet.feridos_graves.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("ilesos",df_parquet.ilesos.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("ignorados",df_parquet.ignorados.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("feridos",df_parquet.feridos.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("veiculos",df_parquet.veiculos.cast(IntegerType()))
    df_parquet = df_parquet.withColumn("latitude", regexp_replace(col("latitude"), ",", ".").cast(DoubleType()))
    df_parquet = df_parquet.withColumn("longitude", regexp_replace(col("longitude"), ",", ".").cast(DoubleType()))
    df_parquet = df_parquet.withColumn("ano",df_parquet.ano.cast(IntegerType()))
    
    print('::group::Casted fields successfully!')
    print(df_parquet.printSchema())
    print('::endgroup::')

    print("Saving data to silver again")
    output_path = "/tmp/silver/car_accidents_casted"

    df_parquet.write \
        .mode("overwrite") \
        .partitionBy("ano") \
        .parquet(output_path)
    
    spark.stop()

if __name__ == "__main__":
    casting_fields()