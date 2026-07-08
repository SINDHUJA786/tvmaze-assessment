# Databricks notebook source
# Storage Configuration
STORAGE_ACCOUNT_NAME = "tvmazestorage12345"
CONTAINER_NAME = "bronze"

# Databricks Secret Scope
STORAGE_ACCOUNT_KEY = dbutils.secrets.get(
    scope="tvmaze-secret-scope",
    key="tvmaze-storage-account-key"
)

# TVMaze API
BASE_URL = "https://api.tvmaze.com"

# Raw Landing Paths
RAW_BASE_PATH = (
    f"abfss://{CONTAINER_NAME}@"
    f"{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw"
)

SHOWS_PATH = f"{RAW_BASE_PATH}/shows"
EPISODES_PATH = f"{RAW_BASE_PATH}/episodes"
CAST_PATH = f"{RAW_BASE_PATH}/cast"

# Catalog / Schema
CATALOG_NAME = "tvmaze_databricks"
SCHEMA_NAME = "tvmaze_bronze"

# Bronze Tables
BRONZE_SHOWS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_shows"
BRONZE_EPISODES_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_episodes"
BRONZE_CAST_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_cast"

# Silver Config
SILVER_SCHEMA = "tvmaze_silver"

SILVER_SHOWS_TABLE = (f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_shows")
SILVER_EPISODES_TABLE = (f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_episodes")
SILVER_CAST_TABLE = (f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_cast")

FACT_SHOW_DATA_TABLE = (f"{CATALOG_NAME}.{SILVER_SCHEMA}.fact_show_data")

# GOLD Config
GOLD_SCHEMA = "tvmaze_gold"

EPISODES_PER_SEASON_TABLE = (f"{CATALOG_NAME}.{GOLD_SCHEMA}.episodes_per_season")
AVG_RUNTIME_PER_SHOW_TABLE = (f"{CATALOG_NAME}.{GOLD_SCHEMA}.avg_runtime_per_show")
TOP_CAST_MEMBERS_TABLE = (f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_cast_members")
MOST_COMMON_GENRES_TABLE = (f"{CATALOG_NAME}.{GOLD_SCHEMA}.most_common_genres")