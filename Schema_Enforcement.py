# Databricks notebook source
# DBTITLE 1,Cell 1
spark.table("tvmaze_gold.top_cast_members")

# COMMAND ----------

# Define expected schema
expected_schema = {
    "person_id": "bigint",
    "person_name": "string",
    "show_count": "bigint",
    "cast_rank": "int"
}

# COMMAND ----------

# reading gold dataframe
gold_df = spark.table("tvmaze_gold.top_cast_members")


# COMMAND ----------

# DBTITLE 1,Cell 3
# compare schema
actual_schema = {
    field.name: field.dataType.simpleString()
    for field in gold_df.schema.fields
}
schema_errors = []
for col_name, expected_type in expected_schema.items():
    if col_name not in actual_schema:
        schema_errors.append(f"Missing column: {col_name}")
    elif actual_schema[col_name] != expected_type:
        schema_errors.append(f"Datatype mismatch for {col_name}."
                            f"Expected {expected_type},"
                            f"Found {actual_schema[col_name]}"
                            )
# check for unexpected columns

extra_columns = set(actual_schema.keys()) - set(expected_schema.keys())

for col in extra_columns:
    schema_errors.append(f"Unexpected column: {col}")

display(actual_schema,expected_schema)

# COMMAND ----------

if schema_errors:
    error_message = "\n".join(schema_errors)
    raise Exception(
        f"""
        GOLD SCHEMA VALIDATION FAILED
        {error_message}""")