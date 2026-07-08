# Databricks notebook source
# MAGIC %run ./config.py

# COMMAND ----------

# MAGIC %run ./Functions.py

# COMMAND ----------

# DBTITLE 1,Cell 3

create_schema(f"{CATALOG_NAME}.{SILVER_SCHEMA}")
configure_silver_settings()

bronze_shows = read_table(BRONZE_SHOWS_TABLE)
bronze_episodes = read_table(BRONZE_EPISODES_TABLE)
bronze_cast = read_table(BRONZE_CAST_TABLE)


silver_shows = (bronze_shows.withColumn("genre",explode_outer("genres"))
    .select(
        col("id").alias("show_id"),
        trim("name").alias("show_name"),
        coalesce(
            col("language"),
            lit("Unknown")
        ).alias("language"),
        "type",
        "status",
        "premiered",
        "runtime",
        "genre",
        col("rating.average").alias("rating_average"),
        col("network.id").alias("network_id"),
        col("network.name").alias("network_name"),
        col("webChannel.id").alias("webchannel_id"),
        col("webChannel.name").alias("webchannel_name")
    )
    .dropDuplicates(["show_id", "genre"])
)

write_delta_table(silver_shows,SILVER_SHOWS_TABLE)

silver_episodes = (bronze_episodes
    .select(
        "show_id",
        col("id").alias("episode_id"),
        "url",
        col("name").alias("episode_name"),
        "season",
        col("number").alias("episode_number"),
        "airdate",
        "runtime",
        coalesce(col("type"),lit("Unknown")).alias("episode_type"),
        col("rating.average").alias("rating_average")
    )
    .dropDuplicates(["episode_id"])
)

write_delta_table(silver_episodes,SILVER_EPISODES_TABLE)


silver_cast = (bronze_cast
    .select(
        "show_id",
        col("person.id").alias("person_id"),
        col("person.name").alias("person_name"),
        col("person.gender").alias("gender"),
        col("person.country.name").alias("country"),
        col("person.birthday").alias("birthday"),
        col("character.name").alias("character_name"),
        "self",
        "voice"
    )
)

write_delta_table(silver_cast,SILVER_CAST_TABLE)

shows_df = read_table(SILVER_SHOWS_TABLE)

episodes_df = (read_table(SILVER_EPISODES_TABLE)
    .withColumn("airdate_date",to_date("airdate"))
    .withColumn("airdate_year",
        coalesce(year("airdate_date"),lit(9999))))

cast_df = read_table(SILVER_CAST_TABLE)

fact_show_data = (
    episodes_df.alias("e")
    .join(broadcast(shows_df.alias("s")),
        "show_id",
        "left")
    .join(
        broadcast(cast_df.alias("c")),
        "show_id",
        "left")
    .select(
        "show_id",
        "show_name",
        "language",
        "genre",
        "season",
        "episode_name",
        "airdate",
        col("e.runtime").alias("runtime"),
        col("person_name").alias("cast_name"),
        "character_name",
        "airdate_year"
    )
    .dropDuplicates()
    .withColumn("_created_at",current_timestamp()
    )
)

(
    fact_show_data.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("airdate_year")
    .saveAsTable(FACT_SHOW_DATA_TABLE)
)

optimize_table(FACT_SHOW_DATA_TABLE)

print("Silver Layer Created")
