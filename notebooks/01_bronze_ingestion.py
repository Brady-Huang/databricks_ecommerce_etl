# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Landing Raw Data
# MAGIC Uses Databricks **Auto Loader** (`cloudFiles`) to continuously monitor the landing zone and incrementally read newly arrived files into Delta tables.
# MAGIC Bronze layer principle: **no cleaning or business-logic transformation**. It only does the following:
# MAGIC - Types are mostly string (keep the data as-is, so dirty data doesn't cause ingestion to fail)
# MAGIC - Adds source metadata: `_source_file`, `_ingest_ts`
# MAGIC - Schema evolution is handled automatically by Auto Loader

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog catalog name")
dbutils.widgets.text("landing_volume_path", "", "Landing zone path (leave empty to derive from catalog)")
dbutils.widgets.text("checkpoint_path", "", "Auto Loader checkpoint path (leave empty to derive from catalog)")

catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_volume_path") or f"/Volumes/{catalog}/raw/landing"
checkpoint_path = dbutils.widgets.get("checkpoint_path") or f"/Volumes/{catalog}/raw/_checkpoints"

TABLES = ["customers", "products", "orders", "order_items"]

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_to_bronze(table_name: str):
    source_path = f"{landing_path}/{table_name}"
    target_table = f"{catalog}.bronze.{table_name}"
    schema_location = f"{checkpoint_path}/{table_name}/_schema"
    checkpoint_location = f"{checkpoint_path}/{table_name}/_checkpoint"
      
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", schema_location)
          .option("cloudFiles.inferColumnTypes", "true")
          .option("header", "true")
          .load(source_path)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingest_ts", F.current_timestamp()))

    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_location)
             .outputMode("append")
             .trigger(availableNow=True)  # Batch-style micro-batch processing: process all new files in this run, then stop
             .toTable(target_table))

    query.awaitTermination()
    print(f"[OK] Bronze ingestion done: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Auto Loader Ingestion Table by Table
# MAGIC Uses `trigger(availableNow=True)`: suited to scheduled batch jobs (each run reads all new data and then finishes).


# COMMAND ----------

for table in TABLES:
    ingest_to_bronze(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Bronze Tables

# COMMAND ----------

for table in TABLES:
    count = spark.table(f"{catalog}.bronze.{table}").count()
    print(f"{catalog}.bronze.{table}: {count} rows")

display(spark.sql(f"SELECT * FROM {catalog}.bronze.orders LIMIT 10"))