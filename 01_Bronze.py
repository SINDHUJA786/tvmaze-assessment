# Databricks notebook source
# DBTITLE 1,Load config and functions
# MAGIC %run ./config.py

# COMMAND ----------

# DBTITLE 1,Load functions
# MAGIC %run ./Functions.py

# COMMAND ----------

# DBTITLE 1,Cell 1
configure_storage(spark)

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}""")

## Load Shows

shows = fetch_api_data(f"{BASE_URL}/shows")
write_json_to_adls(shows,f"{SHOWS_PATH}/shows.json")
print("Shows Loaded")

## Read Show ids.

shows_df = spark.read.json(SHOWS_PATH)
show_ids = [row["id"]for row in shows_df.select("id").collect()]

## Load Episodes

for show_id in show_ids:
    episodes = fetch_api_data(f"{BASE_URL}/shows/{show_id}/episodes")
    write_json_to_adls(episodes,f"{EPISODES_PATH}/show_{show_id}_episodes.json")

print("Episodes Loaded")

## Load Cast

for show_id in show_ids:
    cast = fetch_api_data(f"{BASE_URL}/shows/{show_id}/cast")
    write_json_to_adls(cast,f"{CAST_PATH}/show_{show_id}_cast.json")

print("Cast Loaded")

## Bronze Tables.
shows_df = spark.read.json(SHOWS_PATH)
episodes_df = spark.read.json(EPISODES_PATH)
cast_df = spark.read.json(CAST_PATH)

bronze_shows_df = add_ingestion_metadata(shows_df)
bronze_episodes_df = add_ingestion_metadata(episodes_df)
bronze_cast_df = add_ingestion_metadata(cast_df)

# Extract show_id
bronze_episodes_df = add_show_id(bronze_episodes_df,"episodes")
bronze_cast_df = add_show_id(bronze_cast_df,"cast")

# Delta Tables
save_delta_table(bronze_shows_df,BRONZE_SHOWS_TABLE)
save_delta_table(bronze_episodes_df,BRONZE_EPISODES_TABLE)
save_delta_table(bronze_cast_df,BRONZE_CAST_TABLE)

print("Bronze Layer Created")

