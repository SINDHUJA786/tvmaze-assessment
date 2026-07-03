# Databricks notebook source
storage_account_name = "tvmazestorage12345"
container_name = "bronze"
storage_account_key = "JKOD9ozEFhi7+uh7y49dwfbQ8mPIleTVdUW8MzVjZapaokqx0BajuKS7y/G5n1432YlL4kkJWEwa+AStBlRAqw=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/shows"

# COMMAND ----------

import requests
url = "https://api.tvmaze.com/shows"
response = requests.get(url)
data = response.json()

# COMMAND ----------

import json
json_str = json.dumps(data)

# COMMAND ----------

# DBTITLE 1,Cell 6
output_path = f"{base_path}/shows.json"
dbutils.fs.put(output_path, json_str, overwrite=True)
##display(dbutils.fs.ls(base_path))

# COMMAND ----------

# DBTITLE 1,Cell 7
json_str_1 = dbutils.fs.head(output_path, 10 * 1024 * 1024)
import json
data_1 = json.loads(json_str_1)
show_id = [show['id'] for show in data_1]
##print(show_id[:1000])

# COMMAND ----------

## Episodes Data Load

storage_account_name = "tvmazestorage12345"
container_name = "bronze"
storage_account_key = "JKOD9ozEFhi7+uh7y49dwfbQ8mPIleTVdUW8MzVjZapaokqx0BajuKS7y/G5n1432YlL4kkJWEwa+AStBlRAqw=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/episodes"
import requests
import json

def fetch_episodes(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/episodes"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()



# COMMAND ----------

def write_raw_to_adls(show_id, data, base_path):
    json_str = json.dumps(data)
    output_path = f"{base_path}/show_{show_id}_episodes.json"
    dbutils.fs.put(output_path, json_str, overwrite=True)
    #print(f"written: {output_path}")

# COMMAND ----------

show_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 114, 115, 116, 117, 118, 120, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249]

# COMMAND ----------

for show_id in show_ids:
    ##print(f"show ID: {show_id}")
    episodes = fetch_episodes(show_id)
    write_raw_to_adls(show_id, episodes, base_path)

# COMMAND ----------

display(dbutils.fs.ls(base_path))

# COMMAND ----------

## Fetch Cast Data

cast_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/cast"
import requests
import json

def fetch_cast(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/cast"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def write_cast_to_adls(show_id, data, base_path):
    json_str = json.dumps(data)
    output_path = f"{base_path}/show_{show_id}_cast.json"
    dbutils.fs.put(output_path, json_str, overwrite=True)
    #print(f"written cast Data: {output_path}")

# COMMAND ----------

## Getting IDS from raw shows File

shows_df = spark.read.json("abfss://bronze@tvmazestorage12345.dfs.core.windows.net/raw/shows/")
show_ids = [row['id'] for row in shows_df.select("id").collect()]

# COMMAND ----------

for show_id in show_ids:
    #print(f"Cast for show ID: {show_id}")
    cast = fetch_cast(show_id)
    write_cast_to_adls(show_id, cast, cast_base_path)

#display(dbutils.fs.ls(cast_base_path))



# COMMAND ----------

########################## Step 2 - Creating Delta Lake Bronze Tables ##########################

## Setting Paths...

storage_account_name = "tvmazestorage12345"
container_name = "bronze"

raw_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw"

shows_raw_path = f"{raw_base_path}/shows/"
episodes_raw_path = f"{raw_base_path}/episodes/"
cast_raw_path = f"{raw_base_path}/cast/"


storage_account_key = "JKOD9ozEFhi7+uh7y49dwfbQ8mPIleTVdUW8MzVjZapaokqx0BajuKS7y/G5n1432YlL4kkJWEwa+AStBlRAqw=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

spark.sql("CREATE DATABASE  IF NOT EXISTS tvmaze_bronze")
spark.sql("USE tvmaze_bronze")


# COMMAND ----------

shows_df = spark.read.json(shows_raw_path)
shows_df.printSchema()
#display(shows_df.limit(5))

## Adding Ingestion Metadata to Shows

from pyspark.sql.functions import current_timestamp, input_file_name, col

bronze_shows_df = (
    shows_df
    .withColumn("ingest_file_name", col("_metadata.file_path"))
    .withColumn("ingest_time", current_timestamp())
)

## Save Shows as Managed Delta Table

bronze_shows_df.write.mode("overwrite").format("delta").saveAsTable("bronze_shows")
#display(spark.table("bronze_shows").limit(5))


# COMMAND ----------

# DBTITLE 1,Cell 18
from pyspark.sql.functions import col, current_timestamp

episodes_df = spark.read.json(episodes_raw_path)
episodes_df.printSchema()
#display(episodes_df.limit(5))

## Adding Ingestion Metadata to Episodes

bronze_episodes_df = (
    episodes_df
    .withColumn("ingest_file_name", col("_metadata.file_path"))
    .withColumn("ingest_time", current_timestamp())
)

## Save Episodes as Managed Delta Table

bronze_episodes_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("bronze_episodes")
#display(spark.table("bronze_episodes").limit(5))



# COMMAND ----------


from pyspark.sql.functions import regexp_extract, col

episodes_df = spark.table("tvmaze_databricks.tvmaze_bronze.bronze_episodes")

episodes_df = episodes_df.withColumn(
    "show_id",
    regexp_extract(
        col("ingest_file_name"),
        r"show_(\d+)_episodes",
        1
    ).cast("long")
)

#display(episodes_df.select("show_id", "id", "name", "ingest_file_name"))

# COMMAND ----------

cast_df = spark.read.json(cast_raw_path)
cast_df.printSchema()
#display(cast_df.limit(5))

## Adding Ingestion Metadata to Cast

bronze_cast_df = (
    cast_df
    .withColumn("ingest_file_name", col("_metadata.file_path"))
    .withColumn("ingest_time", current_timestamp())
)

##  Write to Bronze Table
bronze_cast_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_cast")
#display(spark.table("bronze_cast").limit(5))

# COMMAND ----------

## Checking for all Bronze Tables and counts validation

# spark.sql("SHOW TABLES IN tvmaze_bronze").show()
# print("show counts: ", spark.table("bronze_shows").count())
# print("episodes counts: ", spark.table("bronze_episodes").count())
# print("cast counts: ", spark.table("bronze_cast").count())

# COMMAND ----------

from pyspark.sql.types import *

shows_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("language", StringType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("status", StringType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("premiered", StringType(), True),
    StructField("officialSite", StringType(), True),
    StructField("schedule", MapType(StringType(), ArrayType(StringType())), True),
    StructField("rating", MapType(StringType(), FloatType()), True),
    StructField("weight", IntegerType(), True),
    StructField("network", MapType(StringType(), StringType()), True),
    StructField("webChannel", MapType(StringType(), StringType()), True),
    StructField("externals", MapType(StringType(), IntegerType()), True),
    StructField("image", MapType(StringType(), StringType()), True),
    StructField("summary", StringType(), True),
    StructField("updated", IntegerType(), True),
    StructField("_links", MapType(StringType(), MapType(StringType(), StringType())), True)
])

episodes_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("airdate", StringType(), True),
    StructField("airtime", StringType(), True),
    StructField("airstamp", StringType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("image", MapType(StringType(), StringType()), True),
    StructField("summary", StringType(), True),
    StructField("type", StringType(), True),
    StructField("_airtable", MapType(StringType(), StringType()), True)
])

cast_schema = StructType([
    StructField("person", MapType(StringType(), StringType()), True),
    StructField("character", MapType(StringType(), StringType()), True),
    StructField("self", BooleanType(), True),
    StructField("voice", BooleanType(), True),
    StructField("id", IntegerType(), True),
    StructField("show_id", IntegerType(), True),
    StructField("updated", IntegerType(), True)
])

# COMMAND ----------

shows_df = (
    spark.read
        .option("multiLine","true")
        .schema(shows_schema)
        .json(shows_raw_path)
)

episodes_df = (
    spark.read
        .option("multiLine","true")
        .schema(episodes_schema)
        .json(episodes_raw_path)
)

cast_df = (
    spark.read
        .option("multiLine","true")
        .schema(cast_schema)
        .json(cast_raw_path)
)

# COMMAND ----------


spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

bronze_shows_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable("tvmaze_bronze.bronze_shows")

bronze_episodes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable("tvmaze_bronze.bronze_episodes")

bronze_cast_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable("tvmaze_bronze.bronze_cast")


# COMMAND ----------

# MAGIC %sql 
# MAGIC --DESCRIBE DETAIL tvmaze_databricks.tvmaze_bronze.bronze_shows;
# MAGIC --DESCRIBE DETAIL tvmaze_databricks.tvmaze_bronze.bronze_episodes;
# MAGIC --DESCRIBE DETAIL tvmaze_databricks.tvmaze_bronze.bronze_cast;
# MAGIC
# MAGIC --DESCRIBE HISTORY bronze_shows;
# MAGIC --DESCRIBE HISTORY bronze_episodes;
# MAGIC --DESCRIBE HISTORY bronze_cast;

# COMMAND ----------

# DBTITLE 1,Cell 26
# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG tvmaze_databricks TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA tvmaze_databricks.tvmaze_bronze TO `account users`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA tvmaze_databricks.tvmaze_bronze TO `account users`;
# MAGIC
# MAGIC

# COMMAND ----------

## Sample Delta table files

#display(spark.read.format("delta").table("tvmaze_databricks.tvmaze_bronze.bronze_shows").limit(5))
#display(spark.read.format("delta").table("tvmaze_databricks.tvmaze_bronze.bronze_episodes").limit(5))
#display(spark.read.format("delta").table("tvmaze_databricks.tvmaze_bronze.bronze_cast").limit(5))



# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE DETAIL tvmaze_databricks.tvmaze_silver.silver_shows

# COMMAND ----------

# DBTITLE 1,Cell 37
## Adding show_id cols to bronze tables...

from pyspark.sql.functions import regexp_extract, col

spark.table("tvmaze_databricks.tvmaze_bronze.bronze_episodes").withColumn("show_id", regexp_extract(col("ingest_file_name"), r"show_(\d+)_episodes", 1).cast("long")).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("tvmaze_databricks.tvmaze_bronze.bronze_episodes")

spark.table("tvmaze_databricks.tvmaze_bronze.bronze_cast").withColumn("show_id", regexp_extract(col("ingest_file_name"), r"show_(\d+)_cast", 1).cast("long")).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("tvmaze_databricks.tvmaze_bronze.bronze_cast")

# COMMAND ----------

# DBTITLE 1,Cell 34
#display(episodes_df.limit(5))