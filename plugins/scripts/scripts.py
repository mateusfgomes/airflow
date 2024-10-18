import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, concat, lit, when, year
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

def load_data():
    spark = SparkSession.builder.appName('vehicle_accidents').master("local[1]").getOrCreate()
    print('Loading CSV file')

    df = spark.read.format('csv').options(delimiter=';', header='true', infer_schema='true') \
        .load('/tmp/acidentes_brasil.csv')
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

    
def send_to_bronze():
    output_path = "/tmp/bronze/car_accidents"
    new_path = 'datalake/bronze'
    print("Moving data from temp directory to bronze")
    shutil.move(output_path, new_path)
    print("Data moved!")
    

def send_to_silver():
    output_path = "/tmp/silver/car_accidents_enriched"    
    new_path = 'datalake/silver'
    print("Moving data from temp directory to silver")
    shutil.move(output_path, new_path)
    print("Data moved!")