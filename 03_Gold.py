# Databricks notebook source
spark.sql("""CREATE SCHEMA IF NOT EXISTS tvmaze_databricks.tvmaze_gold""")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

shows_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_shows")
episodes_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_episodes")
cast_df = spark.table("tvmaze_databricks.tvmaze_silver.silver_cast")

# COMMAND ----------

##Episodes_per_season

## Aggregation:
episodes_per_season = (
    episodes_df
    .groupBy(
        "show_id",
        "season"
    )
    .agg(
        count("episode_id")
        .alias("episode_count")
    )
)

# COMMAND ----------

## Window Ranking

season_window = Window.partitionBy(
    "show_id"
).orderBy(
    desc("episode_count")
)

episodes_per_season = (
    episodes_per_season
    .withColumn(
        "season_rank",
        dense_rank().over(season_window)
    )
)

# COMMAND ----------

episodes_per_season.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("tvmaze_databricks.tvmaze_gold.episodes_per_season")


# COMMAND ----------

## Avg_runtime_per_show

## Aggregation
avg_runtime_per_show = (
    episodes_df
    .groupBy("show_id")
    .agg(
        round(
            avg("runtime"),
            2
        ).alias("avg_runtime")
    )
)

# COMMAND ----------

## join show names

avg_runtime_per_show = (
    avg_runtime_per_show
    .join(
        shows_df.select(
            "show_id",
            "show_name"
        ).dropDuplicates(),
        "show_id"
    )
)

# COMMAND ----------

## Window Ranking

runtime_window = Window.orderBy(
    desc("avg_runtime")
)

avg_runtime_per_show = (
    avg_runtime_per_show
    .withColumn(
        "runtime_rank",
        rank().over(runtime_window)
    )
)

# COMMAND ----------

avg_runtime_per_show.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("tvmaze_databricks.tvmaze_gold.avg_runtime_per_show")

# COMMAND ----------

## Top_cast_members

## Aggregation
top_cast_members = (
    cast_df
    .groupBy(
        "person_id",
        "person_name"
    )
    .agg(
        countDistinct("show_id")
        .alias("show_count")
    )
)

# COMMAND ----------

## Window Ranking

cast_window = Window.orderBy(
    desc("show_count")
)

top_cast_members = (
    top_cast_members
    .withColumn(
        "cast_rank",
        dense_rank().over(cast_window)
    )
)

# COMMAND ----------

top_cast_members.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("tvmaze_databricks.tvmaze_gold.top_cast_members")

# COMMAND ----------

## Most_Common_Genres

## Aggregation
most_common_genres = (
    shows_df
    .groupBy("genre")
    .agg(
        countDistinct("show_id")
        .alias("show_count")
    )
)

# COMMAND ----------

## window Ranking
genre_window = Window.orderBy(
    desc("show_count")
)

most_common_genres = (
    most_common_genres
    .withColumn(
        "genre_rank",
        dense_rank().over(genre_window)
    )
)

# COMMAND ----------

most_common_genres.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("tvmaze_databricks.tvmaze_gold.most_common_genres")

# COMMAND ----------

## Optimize Gold Tables

spark.sql("""
OPTIMIZE tvmaze_databricks.tvmaze_gold.episodes_per_season
ZORDER BY (show_id, season)
""")


spark.sql("""
OPTIMIZE tvmaze_databricks.tvmaze_gold.avg_runtime_per_show
ZORDER BY (show_id)
""")

spark.sql("""
OPTIMIZE tvmaze_databricks.tvmaze_gold.top_cast_members
ZORDER BY (person_id)
""")

spark.sql("""
OPTIMIZE tvmaze_databricks.tvmaze_gold.most_common_genres
ZORDER BY (genre)
""")


# COMMAND ----------

## Validation

#display(spark.table("tvmaze_databricks.tvmaze_gold.episodes_per_season"))
#display(spark.table("tvmaze_databricks.tvmaze_gold.avg_runtime_per_show"))
#display(spark.table("tvmaze_databricks.tvmaze_gold.top_cast_members"))
#display(spark.table("tvmaze_databricks.tvmaze_gold.most_common_genres"))
