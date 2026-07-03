# Databricks notebook source
spark.sql("""CREATE SCHEMA IF NOT EXISTS tvmaze_databricks.tvmaze_silver""")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_shows = spark.table("tvmaze_databricks.tvmaze_bronze.bronze_shows")
bronze_episodes = spark.table("tvmaze_databricks.tvmaze_bronze.bronze_episodes")
bronze_cast = spark.table("tvmaze_databricks.tvmaze_bronze.bronze_cast")

# COMMAND ----------

silver_shows = (
    bronze_shows
    .withColumn("genre", explode_outer("genres"))
    .select(
        col("id").alias("show_id"),
        trim(col("name")).alias("show_name"),
        coalesce(col("language"), lit("Unknown")).alias("language"),
        col("type"),
        col("status"),
        col("premiered"),
        col("runtime"),
        col("genre"),
        col("rating.average").alias("rating_average"),
        col("network.id").alias("network_id"),
        col("network.name").alias("network_name"),
        col("webChannel.id").alias("webchannel_id"),
        col("webChannel.name").alias("webchannel_name"),
        col("ingest_time"),
        col("ingest_file_name")
    )
)


# COMMAND ----------

silver_shows = silver_shows.dropDuplicates(["show_id","genre"])

# COMMAND ----------

silver_shows.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_shows")

# COMMAND ----------

silver_episodes = (
    bronze_episodes
    .select(
        col("show_id"),
        col("id").alias("episode_id"),
        col("url"),
        col("name").alias("episode_name"),
        col("season"),
        col("number").alias("episode_number"),
        col("airdate"),
        col("runtime"),
        coalesce(col("type"), lit("Unknown")).alias("episode_type"),
        col("rating.average").alias("rating_average"),
        col("ingest_time"),
        col("ingest_file_name")
    )
)

# COMMAND ----------

silver_episodes = silver_episodes.dropDuplicates(["episode_id"])

# COMMAND ----------

silver_episodes.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_episodes")

# COMMAND ----------

bronze_cast = spark.table("tvmaze_databricks.tvmaze_bronze.bronze_cast")

# COMMAND ----------

silver_cast = (
    bronze_cast
    .select(
        col("person.id").alias("person_id"),
        col("person.name").alias("person_name"),
        col("person.gender").alias("gender"),
        col("person.country.name").alias("country"),
        col("person.birthday").alias("birthday"),
        col("character.name").alias("character_name"),
        col("self"),
        col("voice"),
        col("ingest_time"),
        col("ingest_file_name"),
        col("show_id")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 12
silver_cast.withColumnRenamed("trim(character.name AS character_name)", "character_name").write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_cast")

# COMMAND ----------

# print("Null Show IDs:", silver_shows.filter(col("show_id").isNull()).count())
# print("Null Episode IDs:", silver_episodes.filter(col("episode_id").isNull()).count())
# print("Null Person IDs:", silver_cast.filter(col("person_id").isNull()).count())

# COMMAND ----------

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

# COMMAND ----------

# DBTITLE 1,Cell 15
silver_shows.write.format("delta").option("mergeSchema","true").mode("append").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_shows")
silver_episodes.write.format("delta").option("mergeSchema","true").mode("append").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_episodes")
silver_cast.withColumnRenamed("trim(character.name AS character_name)", "character_name").write.format("delta").option("mergeSchema","true").mode("append").saveAsTable("tvmaze_databricks.tvmaze_silver.silver_cast")

# COMMAND ----------

##Fact Table Built..

from pyspark.sql.functions import (
    col,
    broadcast,
    current_timestamp,
    year,
    to_date,
    coalesce,
    lit
)

# ----------------------------------------------------
# Spark performance settings
# ----------------------------------------------------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# ----------------------------------------------------
# Read Silver tables
# ----------------------------------------------------

shows_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_shows")
episodes_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_episodes")
cast_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_cast")

# COMMAND ----------

# ----------------------------------------------------
# Prepare partition column
# ----------------------------------------------------

episodes_df = (
    episodes_df
    .withColumn("airdate_date", to_date(col("airdate")))
    .withColumn("airdate_year", year(col("airdate_date")))
    .withColumn("airdate_year", coalesce(col("airdate_year"), lit(9999)))
)

# COMMAND ----------

# ----------------------------------------------------
# Repartition larger table by join key
# ----------------------------------------------------

episodes_df = episodes_df.repartition(200, "show_id")

# COMMAND ----------

# ----------------------------------------------------
# Build fact table using broadcast joins
# ----------------------------------------------------

fact_show_data = (
    episodes_df.alias("e")
    .join(
        broadcast(shows_df).alias("s"),
        col("e.show_id") == col("s.show_id"),
        "left"
    )
    .join(
        broadcast(cast_df).alias("c"),
        col("e.show_id") == col("c.show_id"),
        "left"
    )

    .select(
        col("s.show_id").alias("show_id"),
        col("s.show_name").alias("show_name"),
        col("s.language").alias("language"),
        col("s.genre").alias("genre"),
        col("e.season").alias("season"),
        col("e.episode_name").alias("episode_name"),
        col("e.airdate").alias("airdate"),
        col("e.runtime").alias("runtime"),
        col("c.person_name").alias("cast_name"),
        col("c.character_name").alias("character_name"),
        col("e.airdate_year").alias("airdate_year")
    )

    .dropDuplicates()

    .withColumn("_created_at", current_timestamp())
)


# COMMAND ----------

# ----------------------------------------------------
# Save as managed Unity Catalog Delta table
# ----------------------------------------------------

fact_show_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("airdate_year") \
    .saveAsTable("tvmaze_databricks.tvmaze_silver.fact_show_data")

# COMMAND ----------

# ----------------------------------------------------
# Optimize FACT table
# ----------------------------------------------------

spark.sql("""
OPTIMIZE tvmaze_databricks.tvmaze_silver.fact_show_data
ZORDER BY (show_id, genre, season, airdate)
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE DETAIL tvmaze_databricks.tvmaze_silver.fact_show_data
# MAGIC