# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - CDC 套用：products (SCD Type 2)
# MAGIC 讀取 `04_products_cdc_simulator` 產生的模擬事件，套用到 `silver.products`。
# MAGIC 邏輯與 orders 相同：關閉舊版本 + 插入新版本。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_products_path", "/Volumes/ecommerce_demo/raw/cdc_landing/products", "CDC Products 事件落地路徑")
dbutils.widgets.text("checkpoint_products_path", "/Volumes/ecommerce_demo/raw/_checkpoints/cdc_products", "Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_products_path")
checkpoint_path = dbutils.widgets.get("checkpoint_products_path")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: 確保 silver.products 已具備 CDC 追蹤欄位
# MAGIC `_cdc_lsn` 用來記錄每筆資料最後一次是被哪個 LSN 更新的。這一步要放在讀取
# MAGIC `last_applied_lsn` 之前執行，否則第一次對全新建立的表執行時，會因為欄位
# MAGIC 還不存在而觸發可避免的例外。

# COMMAND ----------

existing_cols = [f.name for f in spark.table(f"{catalog}.silver.products").schema.fields]
if "_cdc_lsn" not in existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.silver.products ADD COLUMN _cdc_lsn BIGINT")
    print("[OK] 已幫 silver.products 加上 _cdc_lsn 欄位")
else:
    print("[SKIP] _cdc_lsn 欄位已存在")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: 讀取 CDC 事件進 Bronze

# COMMAND ----------

product_before_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("category", StringType(), True),
])

product_after_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("cost", DoubleType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
])

cdc_event_schema = StructType([
    StructField("op", StringType(), True),
    StructField("before", product_before_schema, True),
    StructField("after", product_after_schema, True),
    StructField("source", StructType([
        StructField("table", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("ts_ms", LongType(), True),
    ]), True),
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.products_cdc_log (
    op STRING,
    before STRING,
    after STRING,
    source_table STRING,
    lsn BIGINT,
    ts_ms BIGINT,
    _source_file STRING,
    _ingest_ts TIMESTAMP
) USING DELTA
""")

raw_cdc = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .schema(cdc_event_schema)
           .option("recursiveFileLookup", "true")
           .load(cdc_landing_path))

bronze_cdc = (raw_cdc
              .select(
                  F.col("op"),
                  F.to_json(F.col("before")).alias("before"),
                  F.to_json(F.col("after")).alias("after"),
                  F.col("source.table").alias("source_table"),
                  F.col("source.lsn").alias("lsn"),
                  F.col("source.ts_ms").alias("ts_ms"),
              )
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingest_ts", F.current_timestamp()))

query = (bronze_cdc.writeStream
         .format("delta")
         .option("checkpointLocation", f"{checkpoint_path}/_ingest_checkpoint")
         .outputMode("append")
         .trigger(availableNow=True)
         .toTable(f"{catalog}.bronze.products_cdc_log"))

query.awaitTermination()
print("[OK] CDC events ingested into bronze.products_cdc_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 按 LSN 排序去重

# COMMAND ----------

last_applied_lsn = (spark.table(f"{catalog}.silver.products")
                     .agg(F.max("_cdc_lsn")).collect()[0][0]) or 0

new_events = spark.table(f"{catalog}.bronze.products_cdc_log").filter(F.col("lsn") > F.lit(last_applied_lsn))

w = Window.partitionBy(F.get_json_object("after", "$.product_id")).orderBy(F.col("lsn").desc())

dedup_events = (
    new_events
    .withColumn("product_id", F.get_json_object("after", "$.product_id"))
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select(
        "product_id", "op", "lsn",
        F.get_json_object("after", "$.product_name").alias("product_name"),
        F.get_json_object("after", "$.category").alias("category"),
        F.get_json_object("after", "$.cost").cast("decimal(10,2)").alias("cost"),
        F.get_json_object("after", "$.list_price").cast("decimal(10,2)").alias("list_price"),
        (F.get_json_object("after", "$.is_active") == "true").alias("is_active"),
        F.from_unixtime(F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
    )
)

pending_count = dedup_events.count()
print(f"待套用事件數（去重後）: {pending_count}，上次已套用的 LSN: {last_applied_lsn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: SCD Type 2 套用

# COMMAND ----------

if pending_count > 0:
    # Step 3a：關閉舊版本
    (DeltaTable.forName(spark, f"{catalog}.silver.products").alias("t")
     .merge(dedup_events.alias("s"),
            "t.product_id = s.product_id AND t.is_current = true")
     .whenMatchedUpdate(
         condition="s.op IN ('c','u')",
         set={
             "valid_to": "s.event_ts",
             "is_current": "false",
             "_cdc_lsn": "s.lsn",
         })
     .execute())

    # Step 3b：插入新版本
    new_versions_df = (
        dedup_events
        .withColumn("product_sk", F.expr("uuid()"))
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to", F.lit("9999-12-31 00:00:00").cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_updated_ts", F.current_timestamp())
        .withColumn("_cdc_lsn", F.col("lsn"))
        .select("product_sk", "product_id", "product_name", "category", "cost",
                "list_price", "is_active", "valid_from", "valid_to", "is_current",
                "_updated_ts", "_cdc_lsn")
    )

    new_versions_df.write.mode("append").saveAsTable(f"{catalog}.silver.products")

    print(f"[OK] 已套用 {pending_count} 筆商品 CDC 異動，新增 {new_versions_df.count()} 個新版本")
else:
    print("[SKIP] 沒有新的 CDC 事件需要套用")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 驗證：確認沒有任何 product_id 同時存在兩筆 is_current=true

# COMMAND ----------

display(spark.sql(f"""
    SELECT product_id, COUNT(*) as current_version_count
    FROM {catalog}.silver.products
    WHERE is_current = true
    GROUP BY product_id
    HAVING COUNT(*) > 1
"""))