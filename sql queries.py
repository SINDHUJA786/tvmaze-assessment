# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC -- Agg, Join, group by
# MAGIC SELECT
# MAGIC     e.show_id,
# MAGIC     r.show_name,
# MAGIC     SUM(e.episode_count) AS total_episodes,
# MAGIC     ROUND(AVG(r.avg_runtime),2) AS avg_runtime
# MAGIC FROM tvmaze_databricks.tvmaze_gold.episodes_per_season e
# MAGIC LEFT JOIN tvmaze_databricks.tvmaze_gold.avg_runtime_per_show r
# MAGIC ON e.show_id = r.show_id
# MAGIC GROUP BY
# MAGIC     e.show_id,
# MAGIC     r.show_name
# MAGIC ORDER BY total_episodes DESC;

# COMMAND ----------

# DBTITLE 1,Window Functions on episodes_per_season
# MAGIC %sql
# MAGIC -- Window functions: rank, running total, and per-show average 
# MAGIC SELECT
# MAGIC     show_id,
# MAGIC     season,
# MAGIC     episode_count,
# MAGIC     RANK()        OVER (PARTITION BY show_id ORDER BY episode_count DESC)   AS rank_by_episodes,
# MAGIC     SUM(episode_count) OVER (PARTITION BY show_id ORDER BY season)         AS running_total_episodes,
# MAGIC     ROUND(AVG(episode_count) OVER (PARTITION BY show_id), 2)               AS avg_episodes_per_season
# MAGIC FROM tvmaze_databricks.tvmaze_gold.episodes_per_season
# MAGIC ORDER BY show_id, season;

# COMMAND ----------

# DBTITLE 1,Performance Tuning: Broadcast Hint + Predicate Pushdown
# MAGIC %sql
# MAGIC -- Performance tuning techniques:
# MAGIC -- 1. CTE for early predicate pushdown (filters before join, reduces shuffle data)
# MAGIC -- 2. BROADCAST hint on the smaller filtered table 
# MAGIC -- 3. Window function applied only after the data is already reduced
# MAGIC
# MAGIC WITH top_shows AS (
# MAGIC     -- Predicate pushdown: filter to top 20 shows by runtime BEFORE joining
# MAGIC     -- This shrinks avg_runtime_per_show from ~240 rows to 20 before any shuffle
# MAGIC     SELECT show_id, show_name, avg_runtime
# MAGIC     FROM tvmaze_databricks.tvmaze_gold.avg_runtime_per_show
# MAGIC     WHERE runtime_rank <= 20
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(top_shows) */
# MAGIC     -- BROADCAST: forces the small CTE result into each executor's memory
# MAGIC     -- eliminating the shuffle that a sort-merge join would require
# MAGIC     e.show_id,
# MAGIC     top_shows.show_name,
# MAGIC     e.season,
# MAGIC     e.episode_count,
# MAGIC     top_shows.avg_runtime,
# MAGIC     RANK() OVER (PARTITION BY e.show_id ORDER BY e.episode_count DESC) AS season_rank,
# MAGIC     SUM(e.episode_count) OVER (PARTITION BY e.show_id ORDER BY e.season) AS running_total
# MAGIC FROM tvmaze_databricks.tvmaze_gold.episodes_per_season e
# MAGIC INNER JOIN top_shows
# MAGIC     ON e.show_id = top_shows.show_id
# MAGIC ORDER BY e.show_id, e.season;