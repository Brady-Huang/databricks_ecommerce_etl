# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze 層：原始資料落地
# MAGIC 用 Databricks **Auto Loader**（`cloudFiles`）持續監控 landing zone，把新到的檔案增量讀入 Delta 表。
# MAGIC Bronze 層原則：**不做任何清洗或業務邏輯轉換**，只做：
# MAGIC - 型別是 string 為主（先保留原樣，避免髒資料讓 ingestion 失敗）
# MAGIC - 加上來源中繼資料：`_source_file`、`_ingest_ts`
# MAGIC - Schema 演進交給 Auto Loader 自動處理

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("landing_volume_path", "/Volumes/ecommerce_demo/raw/landing", "Landing Zone 路徑")
dbutils.widgets.text("checkpoint_path", "/Volumes/ecommerce_demo/raw/_checkpoints", "Auto Loader Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_volume_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

TABLES = ["customers", "products", "orders", "order_items", "web_events"]

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
             .trigger(availableNow=True)  # 批次式微批處理：這次執行把所有新檔案處理完就停
             .toTable(target_table))

    query.awaitTermination()
    print(f"[OK] Bronze ingestion done: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 逐表執行 Auto Loader ingestion
# MAGIC 使用 `trigger(availableNow=True)`：適合排程式批次 job（每次執行把新資料讀完就結束），
# MAGIC 如果要做成真正的即時串流，把 trigger 拿掉即可，並讓 job 一直跑著。

# COMMAND ----------

for table in TABLES:
    ingest_to_bronze(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 檢查 Bronze 表

# COMMAND ----------

for table in TABLES:
    count = spark.table(f"{catalog}.bronze.{table}").count()
    print(f"{catalog}.bronze.{table}: {count} rows")

display(spark.sql(f"SELECT * FROM {catalog}.bronze.orders LIMIT 10"))
