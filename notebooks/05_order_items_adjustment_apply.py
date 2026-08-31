# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - 套用訂單明細調整事件：order_items
# MAGIC 讀取 `04_order_items_adjustment_simulator` 產生的調整事件，寫入獨立的
# MAGIC `silver.order_item_adjustments` 表。這裡**不對原始 `silver.order_items` 做任何修改**，
# MAGIC 原始交易記錄保持不可竄改；下游若要計算「某筆明細目前的真實數量/金額」，
# MAGIC 用原始 order_items LEFT JOIN 這張調整表做加總。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_order_items_path", "/Volumes/ecommerce_demo/raw/cdc_landing/order_items", "調整事件落地路徑")
dbutils.widgets.text("checkpoint_order_items_path", "/Volumes/ecommerce_demo/raw/_checkpoints/order_items_adjustments", "Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_order_items_path")
checkpoint_path = dbutils.widgets.get("checkpoint_order_items_path")

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: 建立目標表（若不存在）

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.order_item_adjustments (
    adjustment_id STRING NOT NULL,
    order_item_id STRING,

    order_id STRING,
    adjustment_type STRING,
    quantity_delta INT,
    amount_delta DECIMAL(12,2),
    reason STRING,
    adjusted_at TIMESTAMP,
    source_lsn BIGINT,
    _ingest_ts TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 讀取調整事件（手動指定 schema）

# COMMAND ----------

adjustment_schema = StructType([
    StructField("adjustment_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("adjustment_type", StringType(), True),
    StructField("quantity_delta", IntegerType(), True),
    StructField("amount_delta", DoubleType(), True),
    StructField("reason", StringType(), True),
    StructField("source", StructType([
        StructField("table", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("ts_ms", LongType(), True),
    ]), True),
])


raw_adjustments = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .schema(adjustment_schema)
                    .option("recursiveFileLookup", "true")
                    .load(cdc_landing_path))

adjustments_df = (raw_adjustments
                   .select(
                       "adjustment_id", "order_item_id", "order_id", "adjustment_type",
                       "quantity_delta",
                       F.col("amount_delta").cast("decimal(12,2)").alias("amount_delta"),
                       "reason",
                       F.from_unixtime(F.col("source.ts_ms") / 1000).cast("timestamp").alias("adjusted_at"),
                       F.col("source.lsn").alias("source_lsn"),
                   )
                   .withColumn("_ingest_ts", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Append-only 寫入
# MAGIC 這裡不需要 MERGE，也不需要 LSN 排序去重——因為每一筆調整事件都是獨立的
# MAGIC 歷史紀錄（就像銀行帳本的每一筆交易），不會有「覆蓋」或「取代」的概念，
# MAGIC 直接 append 即可，Auto Loader 本身的 checkpoint 機制已經保證不會重複處理同一個檔案。

# COMMAND ----------

query = (adjustments_df.writeStream
         .format("delta")
         .option("checkpointLocation", f"{checkpoint_path}/_ingest_checkpoint")

         .outputMode("append")
         .trigger(availableNow=True)
         .toTable(f"{catalog}.silver.order_item_adjustments"))

query.awaitTermination()

total_count = spark.table(f"{catalog}.silver.order_item_adjustments").count()
print(f"[OK] order_item_adjustments 累積總筆數: {total_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 驗證：計算調整後的真實明細金額
# MAGIC 示範下游怎麼用原始 order_items LEFT JOIN 調整表，算出「考慮所有異動後」的真實數字，
# MAGIC 而不需要修改任何一筆原始交易記錄。

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        oi.order_item_id,
        oi.quantity AS original_quantity,
        oi.line_total AS original_line_total,
        COALESCE(SUM(adj.quantity_delta), 0) AS total_quantity_adjustment,
        COALESCE(SUM(adj.amount_delta), 0) AS total_amount_adjustment,
        oi.quantity + COALESCE(SUM(adj.quantity_delta), 0) AS effective_quantity,
        oi.line_total + COALESCE(SUM(adj.amount_delta), 0) AS effective_line_total
    FROM {catalog}.silver.order_items oi
    LEFT JOIN {catalog}.silver.order_item_adjustments adj
        ON oi.order_item_id = adj.order_item_id
    GROUP BY oi.order_item_id, oi.quantity, oi.line_total
    HAVING COUNT(adj.adjustment_id) > 0

    LIMIT 20
"""))