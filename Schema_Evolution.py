# Databricks notebook source
# MAGIC %run ./config.py

# COMMAND ----------

# MAGIC
# MAGIC %run ./Functions.py

# COMMAND ----------

# without schema evolution
def write_delta_table(df: DataFrame,table_name: str,mode: str = "overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(table_name)
    )


# COMMAND ----------

# DBTITLE 1,Cell 3

create_schema(f"{CATALOG_NAME}.{SILVER_SCHEMA}")
configure_silver_settings()

bronze_cast = read_table(BRONZE_CAST_TABLE)

silver_cast = (bronze_cast
    .select(
        "show_id",
        col("person.id").alias("person_id"),
        col("person.name").alias("person_name"),
        ## Schema Evolution Testing - Before
        concat_ws("_",col("person.id").cast("string"),col("person.name")).alias("person_uid"),
        col("person.gender").alias("gender"),
        col("person.country.name").alias("country"),
        col("person.birthday").alias("birthday"),
        col("character.name").alias("character_name"),
        "self",
        "voice"
    )
)

write_delta_table(silver_cast,SILVER_CAST_TABLE)

cast_df = read_table(SILVER_CAST_TABLE)


print("Failed because Schema Evolution not Implemented")


# COMMAND ----------

## Schema Evolution Implemented
def write_delta_table(df: DataFrame,table_name: str,mode: str = "overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )

# COMMAND ----------


create_schema(f"{CATALOG_NAME}.{SILVER_SCHEMA}")
configure_silver_settings()

bronze_cast = read_table(BRONZE_CAST_TABLE)

silver_cast = (bronze_cast
    .select(
        "show_id",
        col("person.id").alias("person_id"),
        col("person.name").alias("person_name"),
        ## Schema Evolution Testing - After
        concat_ws("_",col("person.id").cast("string"),col("person.name")).alias("person_uid"),
        col("person.gender").alias("gender"),
        col("person.country.name").alias("country"),
        col("person.birthday").alias("birthday"),
        col("character.name").alias("character_name"),
        "self",
        "voice"
    )
)

write_delta_table(silver_cast,SILVER_CAST_TABLE)

cast_df = read_table(SILVER_CAST_TABLE)


print("Success - Schema Evolution Implemented")