#from dask.dataframe.methods import drop_columns
from idlelib.replace import replace

from dotenv import load_dotenv, find_dotenv
import os

from parsel.xpathfuncs import regex
from pyspark.sql.functions import  *
from pyspark.sql.types import *
from pyspark.sql import *
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
    .getOrCreate()

DB_HOST_PROD = get_env('DB_HOST_PROD')
DB_PORT_PROD = get_env('DB_PORT_PROD')
DB_NAME_PROD = get_env('DB_NAME_PROD')
DB_USER_PROD = get_env('DB_USER_PROD')
DB_PASS_PROD = get_env('DB_PASS_PROD')

URL_PROD = f"jdbc:postgresql://{DB_HOST_PROD}:{DB_PORT_PROD}/{DB_NAME_PROD}"

hostname = "localhost"
port = 3306
database = "test"

jdbc_url = f"jdbc:mysql://{hostname}:{port}/{database}"

df_spark = spark.read.format("jdbc") \
    .option("url", URL_PROD) \
    .option("dbtable", "monster_data_full") \
    .option("user", DB_USER_PROD) \
    .option("password", DB_PASS_PROD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

#df_spark = spark.read.csv('renew_monster_data',header=True)

schema = ArrayType(MapType(StringType(), StringType()))
df_spark1 = df_spark.withColumn("drops", from_json(col("drops"), schema))
df_exploded1 = df_spark1.withColumn("drop", explode("drops"))
df_selected1 = df_exploded1.select(col("_id").alias('mob_db_id'),
                                 col("monster_id").alias('monster_id'),
                                 col("drop.name").alias("drop_name"),
                                 col("drop.img").alias("drop_img"),
                                col("drop.rate").alias("drop_rate"),
                                 col("element_power"))

df_selected1.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_drops") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

df_selected1.show()
print('\n')

schema2 = MapType(StringType(), StringType())
df_spark2 = df_spark.withColumn("main_atb", from_json(col("main_atb"), schema2))
def_spark2 = df_spark2.select(col("_id").alias('mob_db_id'),
                                 col("monster_id").alias('monster_id'),
                                 col("main_atb").getItem("agi").alias("mob_agi"),
                                col("main_atb").getItem("int").alias("mob_int"),
                                col("main_atb").getItem("luk").alias("mob_luk"),
                                col("main_atb").getItem("vit").alias("mob_vit"),
                                col("main_atb").getItem("dex").alias("mob_dex"))
def_spark2.show()

def_spark2.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_main_atb") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

schema3 = MapType(StringType(),StringType())
def_spark3 = df_spark.withColumn('main_stats', from_json(col('main_stats'),schema3))
def_spark3 = def_spark3.select(col('_id').alias('mob_db_id'),
                               col('monster_id').alias('monster_id'),
                               col('main_stats').getItem('level').alias('mob_level'),
                               col('main_stats').getItem('def').alias('mob_def'),
                               col('main_stats').getItem('hp').alias('mob_hp'),
                               col('main_stats').getItem('m_def').alias('mob_m_def'),
                               col('main_stats').getItem('attack').alias('mob_atk'),
                               col('main_stats').getItem('range').alias('mob_range'),
                               col('main_stats').getItem('aspd').alias('mob_aspd'),
                               col('main_stats').getItem('move_speed').alias('mob_move_speed'),
                               col('main_stats').getItem('base_exp').alias('mob_base_exp'),
                               col('main_stats').getItem('base_exp_per_hp').alias('mob_base_xphp'),
                               col('main_stats').getItem('job_exp').alias('mob_job_exp'),
                               col('main_stats').getItem('job_exp_per_hp').alias('mob_job_xphp'),
                               col('main_stats').getItem('hit').alias('mob_hit'),
                               col('main_stats').getItem('flee').alias('mob_flee')
                               )
def_spark3 = def_spark3.withColumn('mob_range',regexp_replace('mob_range',' cell',''))
def_spark3 = def_spark3.withColumn('mob_atk_min', split(col('mob_atk'),'~').getItem(0))\
                                    .withColumn('mob_def',regexp_replace(col('mob_def'),r'\+','~'))\
                                    .withColumn('mob_atk_max', split(col('mob_atk'),'~').getItem(1))\
                                    .withColumn('mob_def_base',split(col('mob_def'),'~').getItem(0))\
                                    .withColumn('mob_def_aditional',split(col('mob_def'),'~').getItem(1))\
                                    .drop('mob_def')\
                                    .drop('mob_atk')
def_spark3.show()

def_spark3.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_main_stats") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

schema4 = MapType(StringType(), StringType())
def_spark4 = df_spark.withColumn('elementalDamage',from_json(col('elementalDamage'),schema4))
def_spark4 = def_spark4.select(col('_id').alias('mob_db_id'),
                             col('monster_id').alias('monster_id'),
                             col('elementalDamage').getItem('neutral').alias('mob_element_neutral'),
                             col('elementalDamage').getItem('poison').alias('mob_element_poison'),
                             col('elementalDamage').getItem('earth').alias('mob_element_earth'),
                             col('elementalDamage').getItem('shadow').alias('mob_element_shadow'),
                             col('elementalDamage').getItem('water').alias('mob_element_water'),
                             col('elementalDamage').getItem('undead').alias('mob_element_undead'),
                             col('elementalDamage').getItem('fire').alias('mob_element_fire'),
                             col('elementalDamage').getItem('holy').alias('mob_element_holy'),
                             col('elementalDamage').getItem('wind').alias('mob_element_wind'),
                             col('elementalDamage').getItem('ghost').alias('mob_element_ghost'),
                             )
def_spark4.show()

def_spark4.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_elemental_dmg") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

schema5 = MapType(StringType(),StringType())
def_spark5 = df_spark.withColumn('skills',from_json(col('skills'),schema5))
def_spark5 = def_spark5.select(col('_id').alias('mob_db_id'),
                               col('monster_id'),
                               col('skills').getItem('mode').alias('mob_skill_mode'),
                               col('skills').getItem('spell').alias('mob_skill_spells'),
                               col('skills').getItem('summon').alias('mob_skill_summons'))
def_spark5.show()

def_spark5.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_skills") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

def_spark_main = df_spark.withColumn('monster_info', initcap(regexp_replace(col('monster_info'),'_', ' ')))\
    .withColumn('monster_size',initcap('size'))\
    .withColumn('monster_race',initcap('race'))\
    .withColumn('monster_type',initcap('type'))
def_spark_main1 = def_spark_main.select(col('_id').alias('mob_db_id')
                                  ,col('monster_id'),
                                  col('monster_info').alias('mob_info'),
                                  col('monster_size').alias('mob_size'),
                                  col('monster_race').alias('mob_race'),
                                  col('monster_type').alias('mob_type'),
                                  col('element_power').alias('mob_element_lvl'),
                                 col('gif').alias('monster_gif'))

def_spark_main1.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_main") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

schema6 = StructType([
    StructField("mode", ArrayType(StringType()), True),
    StructField("spell", ArrayType(StructType([StructField("level", IntegerType(), True),StructField("name", StringType(), True)])), True),
    StructField("summon", StructType([StructField("summoned_by", ArrayType(StringType()), True)]), True)])

df_spark6 = df_spark.withColumn("skill", from_json(col("skills"), schema6))

df_spark6 = df_spark6.withColumn("skill_modes", explode(col("skill.mode"))) \
    .withColumn("skill_modes", regexp_replace(col("skill_modes"), '_', ' ')) \
    .withColumn("skill_modes", initcap(col("skill_modes")))\
    .withColumn("skill_spells",explode(col("skill.spell")))\
    .withColumn("skill_spell_lvl",col("skill_spells").getItem("level"))\
    .withColumn("skill_spell_name",col("skill_spells").getItem("name"))\
    .withColumn("skill_spell_name",regexp_replace(col("skill_spell_name"),'_',' '))\
    .withColumn('skill_spell_name',initcap(col("skill_spell_name")))\
    .withColumn("mob_summoned_by",col("skill.summon").getItem("summoned_by"))


df_spark6 = df_spark6.select(col("_id").alias('mob_db_id'),col("monster_id"),col("skill_modes"),col("skill_spell_lvl"),col("skill_spell_name"),col("mob_summoned_by"))

df_spark6.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_skills") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()

schema7 = ArrayType(MapType(StringType(),StringType()))
def_spark7 = df_spark.withColumn("mob_maps",from_json(col("maps"),schema7))
def_spark7 = def_spark7.withColumn("mob_maps",explode(col("mob_maps")))
def_spark7 = def_spark7.select(col("_id").alias('mob_db_id'),col("monster_id"),
                               col("mob_maps.amount"),
                               col("mob_maps.frequency"),
                               col("mob_maps.name"),
                               col("mob_maps.number"),
                               col("mob_maps.type"),
                               col("mob_maps.img"))

def_spark7 = def_spark7.withColumn("map_frequency",initcap(regexp_replace(col("frequency"),"_",' ')))\
    .withColumn("map_name",initcap(regexp_replace(col("name"),"_"," ")))\
    .withColumn("map_type",initcap(regexp_replace(col("type"),"_"," ")))

def_spark7 = def_spark7.select(col("mob_db_id"),col("monster_id"),col("amount"),col("map_frequency"),col("map_name"),col("map_type"),col("img").alias("map_img"))

def_spark7.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "monster_map_spawn") \
    .option("user", 'root') \
    .option("password", "") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .save()
