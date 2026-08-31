# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - CDC 套用：orders (SCD Type 2)
# MAGIC 讀取 `04_orders_cdc_simulator` 產生的模擬事件，套用到 `silver.orders`。
# MAGIC
# MAGIC 與 customers（SCD1）不同，這裡採用 **SCD Type 2**：每次狀態異動不是覆蓋舊資料，
# MAGIC 而是「關閉舊版本」+「插入新版本」，保留完整的狀態變更歷史。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_orders_path", "/Volumes/ecommerce_demo/raw/cdc_landing/orders", "CDC Orders 事件落地路徑")
dbutils.widgets.text("checkpoint_orders_path", "/Volumes/ecommerce_demo/raw/_checkpoints/cdc_orders", "Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_orders_path")
checkpoint_path = dbutils.widgets.get("checkpoint_orders_path")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: 確保 silver.orders 已具備 CDC 追蹤欄位
# MAGIC `_cdc_lsn` 用來記錄每筆資料最後一次是被哪個 LSN 更新的，讓下次執行 CDC 時
# MAGIC 知道「上次同步到哪裡了」。這一步要放在讀取 `last_applied_lsn` 之前執行，
# MAGIC 否則第一次對全新建立的表執行時，會因為欄位還不存在而觸發可避免的例外。

# COMMAND ----------

existing_cols = [f.name for f in spark.table(f"{catalog}.silver.orders").schema.fields]
if "_cdc_lsn" not in existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.silver.orders ADD COLUMN _cdc_lsn BIGINT")
    print("[OK] 已幫 silver.orders 加上 _cdc_lsn 欄位")
else:
    print("[SKIP] _cdc_lsn 欄位已存在")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: 讀取 CDC 事件進 Bronze
# MAGIC 手動指定 schema（不靠 Auto Loader 自動推斷），因為 before/after 欄位數量不對稱
# MAGIC （新增事件 before 為 null，更新事件 before 只帶被改動的欄位）會讓 schema 推斷不穩定。

# COMMAND ----------

# before 只會出現在 update 事件，只帶 order_id + status（對照 04 模擬器裡 before 的組成）
order_before_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("status", StringType(), True),
])

# after 帶完整欄位（不管是 create 還是 update 都是完整的訂單快照）
order_after_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_total", DoubleType(), True),
])

cdc_event_schema = StructType([
    StructField("op", StringType(), True),
    StructField("before", order_before_schema, True),
    StructField("after", order_after_schema, True),
    StructField("source", StructType([
        StructField("table", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("ts_ms", LongType(), True),
    ]), True),
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.orders_cdc_log (
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

         .toTable(f"{catalog}.bronze.orders_cdc_log"))

query.awaitTermination()
print("[OK] CDC events ingested into bronze.orders_cdc_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 按 LSN 排序去重，找出真正要套用的異動
# MAGIC 只處理比上次同步點更新的事件；同一張訂單若有多筆異動，只保留 LSN 最大的那一筆。

# COMMAND ----------

try:
    last_applied_lsn = (spark.table(f"{catalog}.silver.orders")
                         .agg(F.max("_cdc_lsn")).collect()[0][0]) or 0
except Exception:
    last_applied_lsn = 0

new_events = spark.table(f"{catalog}.bronze.orders_cdc_log").filter(F.col("lsn") > F.lit(last_applied_lsn))

w = Window.partitionBy(F.get_json_object("after", "$.order_id")).orderBy(F.col("lsn").desc())

dedup_events = (
    new_events
    .withColumn("order_id", F.get_json_object("after", "$.order_id"))
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select(
        "order_id", "op", "lsn",
        F.get_json_object("after", "$.customer_id").alias("customer_id"),
        F.to_timestamp(F.get_json_object("after", "$.order_date")).alias("order_date"),

        F.get_json_object("after", "$.status").alias("status"),
        F.get_json_object("after", "$.payment_method").alias("payment_method"),
        F.get_json_object("after", "$.order_total").cast("decimal(12,2)").alias("order_total"),
        F.from_unixtime(F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
    )
)

pending_count = dedup_events.count()
print(f"待套用事件數（去重後）: {pending_count}，上次已套用的 LSN: {last_applied_lsn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: SCD Type 2 套用 —— 關閉舊版本 + 插入新版本
# MAGIC 這是與 customers（SCD1）最大的差異所在。SCD1 用單一 MERGE 直接覆蓋；
# MAGIC SCD2 需要兩個步驟：先把目前生效版本標記失效，再插入一筆全新版本。

# COMMAND ----------

if pending_count > 0:
    # 需要有 _cdc_lsn 欄位追蹤同步進度，跟 customers 的做法一致
    existing_cols = [f.name for f in spark.table(f"{catalog}.silver.orders").schema.fields]
    if "_cdc_lsn" not in existing_cols:
        spark.sql(f"ALTER TABLE {catalog}.silver.orders ADD COLUMN _cdc_lsn BIGINT")
        print("[OK] 已幫 silver.orders 加上 _cdc_lsn 欄位")

    # Step 3a：關閉舊版本 —— 找到目前 current 且這次有異動的訂單，標記失效
    (DeltaTable.forName(spark, f"{catalog}.silver.orders").alias("t")
     .merge(dedup_events.alias("s"),
            "t.order_id = s.order_id AND t.is_current = true")
     .whenMatchedUpdate(
         condition="s.op IN ('c','u')",  # create 事件理論上不會 match 到既有 current 版本，這裡主要處理 update

         set={
             "valid_to": "s.event_ts",
             "is_current": "false",
             "_cdc_lsn": "s.lsn",
         })
     .execute())

    # Step 3b：插入新版本 —— 每一筆異動都產生一筆全新的 current 版本
    new_versions_df = (
        dedup_events
        .withColumn("order_sk", F.expr("uuid()"))
        .withColumn("is_valid_customer", F.lit(None).cast("boolean"))  # CDC 事件暫不重新驗證 FK，沿用批次的檢查邏輯
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to", F.lit("9999-12-31 00:00:00").cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_updated_ts", F.current_timestamp())
        .withColumn("_cdc_lsn", F.col("lsn"))
        .select("order_sk", "order_id", "customer_id", "order_date", "status", "payment_method",
                "order_total", "is_valid_customer", "valid_from", "valid_to", "is_current",
                "_updated_ts", "_cdc_lsn")
    )

    new_versions_df.write.mode("append").saveAsTable(f"{catalog}.silver.orders")

    print(f"[OK] 已套用 {pending_count} 筆訂單 CDC 異動，新增 {new_versions_df.count()} 個新版本")
else:
    print("[SKIP] 沒有新的 CDC 事件需要套用")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 檢查結果：orders 的版本歷史


# COMMAND ----------

display(spark.sql(f"""
    SELECT order_id, status, valid_from, valid_to, is_current, _cdc_lsn
    FROM {catalog}.silver.orders
    ORDER BY order_id, valid_from
    LIMIT 20
"""))