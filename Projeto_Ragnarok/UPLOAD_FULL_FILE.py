from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv, find_dotenv
import findspark

#inicialização dos arquivos de ambiente do .env
load_dotenv(find_dotenv())

#inicialização Findspark para identificar o diretório correto do spark
findspark.init()

#definindo o get_env
def get_env(key, default=None):
    value = os.getenv(key)
    return default if value is None else value

#inicialização do SparkSession
spark = SparkSession.builder \
    .appName("UPLOAD_FULL_RAG_") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .config("spark.executor.extraClassPath", "C:\\Users\\Fernando\\spark_install\\spark-3.4.4-bin-hadoop3\\postgresql-42.7.0.jar") \
    .config("spark.driver.extraClassPath", "C:\\Users\\Fernando\\spark_install\\spark-3.4.4-bin-hadoop3\\postgresql-42.7.0.jar") \
    .getOrCreate()

DB_HOST_PROD = get_env('DB_HOST_PROD')
DB_PORT_PROD = get_env('DB_PORT_PROD')
DB_NAME_PROD = get_env('DB_NAME_PROD')
DB_USER_PROD = get_env('DB_USER_PROD')
DB_PASS_PROD = get_env('DB_PASS_PROD')
DB_SCHEMA_PROD = get_env('DB_SCHEMA_PROD')
DB_THREADS_PROD = get_env('DB_THREADS_PROD')
DB_TYPE_PROD = get_env('DB_TYPE_PROD')
DB_PROFILES_PROD = get_env('DB_PROFILES_PROD')

URL_PROD = f"jdbc:postgresql://{DB_HOST_PROD}:{DB_PORT_PROD}/{DB_NAME_PROD}"

df_spark = spark.read.csv('renew_monster_data', header=True, inferSchema=True,)
df_spark.show()
df_spark.write.format("jdbc") \
    .option("url", URL_PROD) \
    .option("dbtable", "monster_data_full") \
   .option("user", DB_USER_PROD) \
   .option("password", DB_PASS_PROD) \
    .option("driver", "org.postgresql.Driver") \
    .save()