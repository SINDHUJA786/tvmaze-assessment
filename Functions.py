# Databricks notebook source
# DBTITLE 1,Cell 1
import json
import requests
from pyspark.sql.functions import (current_timestamp,col,regexp_extract)
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


try:    _config_src = dbutils.fs.head("/Workspace/Users/sindhujabatkeeri.7@gmail.com/config.py", 1 << 20)
except Exception:
    _config_src = ''  # or handle accordingly, e.g., load from secrets or environment variables

if _config_src:    exec(compile(_config_src, "config.py", "exec"), globals())


# ADLS Configuration
def configure_storage(spark):
    spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",STORAGE_ACCOUNT_KEY)

# API Call
def fetch_api_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Writing JSON to ADLS
def write_json_to_adls(data, file_path):
    json_str = json.dumps(data)
    dbutils.fs.put(file_path,json_str,overwrite=True)

# Ingestion Metadata
def add_ingestion_metadata(df):
    return (df.withColumn("ingest_file_name",col("_metadata.file_path")).withColumn("ingest_time",current_timestamp()))

# Save Delta Table
def save_delta_table(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )

# Add show_id from filename
def add_show_id(df, entity):
    return (df.withColumn("show_id",regexp_extract(col("ingest_file_name"), rf"show_(\d+)_{entity}",1).cast("long")))

def create_schema(schema_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

def read_table(table_name):
    return spark.table(table_name)

def configure_silver_settings():
    spark.conf.set("spark.sql.adaptive.enabled","true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled","true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled","true")
    spark.conf.set("spark.sql.shuffle.partitions","200")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

def write_delta_table(df: DataFrame,table_name: str,mode: str = "overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )

def optimize_table(tablename):
    spark.sql(f"""OPTIMIZE {tablename} ZORDER BY (show_id)""")

def build_episodes_per_season(df):
    season_window = (Window.partitionBy("show_id").orderBy(desc("episode_count")))

    return (df.groupBy("show_id","season")
        .agg(count("episode_id").alias("episode_count"))
        .withColumn("season_rank",dense_rank().over(season_window)))

def build_avg_runtime_per_show(episodes_df,shows_df):
    runtime_window = Window.orderBy(desc("avg_runtime"))

    return (episodes_df.groupBy("show_id").agg(round(avg("runtime"),2).alias("avg_runtime"))
        .join(shows_df.select("show_id","show_name").dropDuplicates(),"show_id")
        .withColumn("runtime_rank",rank().over(runtime_window)))

def build_top_cast_members(df):
    cast_window = Window.orderBy(desc("show_count"))
    return (df.groupBy("person_id","person_name").agg(countDistinct("show_id").alias("show_count")).withColumn("cast_rank",dense_rank().over(cast_window)))

def build_most_common_genres(df):
    genre_window = Window.orderBy(desc("show_count"))
    return (df.groupBy("genre").agg(countDistinct("show_id").alias("show_count")).withColumn("genre_rank",dense_rank().over(genre_window)))

def write_to_gold(df,table_name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(table_name))

def optimize_gold_tables():
    spark.sql(f"""OPTIMIZE {EPISODES_PER_SEASON_TABLE} ZORDER BY (show_id, season)""")
    spark.sql(f"""OPTIMIZE {AVG_RUNTIME_PER_SHOW_TABLE} ZORDER BY (show_id)""")
    spark.sql(f"""OPTIMIZE {TOP_CAST_MEMBERS_TABLE} ZORDER BY (person_id)""")
    spark.sql(f"""OPTIMIZE {MOST_COMMON_GENRES_TABLE} ZORDER BY (genre)""")

