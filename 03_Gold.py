# Databricks notebook source
# MAGIC %run ./config.py

# COMMAND ----------

# MAGIC %run ./Functions.py

# COMMAND ----------

create_schema(f"{CATALOG_NAME}.{GOLD_SCHEMA}")

shows_df = read_table(SILVER_SHOWS_TABLE)
episodes_df = read_table(SILVER_EPISODES_TABLE)
cast_df = read_table(SILVER_CAST_TABLE)

episodes_per_season = (build_episodes_per_season(episodes_df))
avg_runtime_per_show = (build_avg_runtime_per_show(episodes_df,shows_df))
top_cast_members = (build_top_cast_members(cast_df))
most_common_genres = (build_most_common_genres(shows_df))

write_to_gold(episodes_per_season,EPISODES_PER_SEASON_TABLE)
write_to_gold(avg_runtime_per_show,AVG_RUNTIME_PER_SHOW_TABLE)
write_to_gold(top_cast_members,TOP_CAST_MEMBERS_TABLE)
write_to_gold(most_common_genres,MOST_COMMON_GENRES_TABLE)

optimize_gold_tables()

print("Gold Layer Created")
