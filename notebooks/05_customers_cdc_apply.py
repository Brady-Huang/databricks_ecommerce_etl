# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - CDC 套用：把 Postgres 的變更同步到 silver.customers
# MAGIC 這個 notebook 做兩件事：
# MAGIC 1. **Bronze**：用 Auto Loader 把 CDC JSON 事件增量讀進 `bronze.customers_cdc_log`（append-only，保留完整變更歷史，方便追溯/重播）
# MAGIC 2. **Apply**：使用 **Streaming Checkpoint 增量讀取** 搭配 **foreachBatch**，在同一批次內先取 LSN 最大的那筆事件，
# MAGIC    再用 `MERGE INTO` 套用到 `silver.customers`。由 SQL MERGE 逐行比對特性，確保新事件的 LSN 大於已套用的 LSN 時才更新。
# MAGIC    這樣既解決了全表掃描的效能問題，也能完美防禦快慢車與亂序覆蓋，做到真正的 Idempotent（冪等性）。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_path", "/Volumes/ecommerce_demo/raw/cdc_landing/customers", "CDC 事件落地路徑")
dbutils.widgets.text("checkpoint_path", "/Volumes/ecommerce_demo/raw/_checkpoints/cdc_customers", "Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Bronze - 讀入原始 CDC 事件 (append-only log)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# 手動定義 schema，避免 Spark 因為 before/after 欄位數量不一致而誤判
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("is_member", BooleanType(), True),
    StructField("age", LongType(), True),
])

cdc_event_schema = StructType([
    StructField("op", StringType(), True),
    StructField("before", customer_schema, True),
    StructField("after", customer_schema, True),
    StructField("source", StructType([
        StructField("table", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("ts_ms", LongType(), True),
    ]), True),
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.customers_cdc_log (
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
         .toTable(f"{catalog}.bronze.customers_cdc_log"))

query.awaitTermination()
print("[OK] CDC events ingested into bronze.customers_cdc_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 確保 silver.customers 有追蹤 CDC 進度用的欄位
# MAGIC `_cdc_lsn`：這筆資料最後一次被套用時的來源 LSN，用來擋掉「比較舊」的事件。

# COMMAND ----------

existing_cols = [f.name for f in spark.table(f"{catalog}.silver.customers").schema.fields]
if "_cdc_lsn" not in existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.silver.customers ADD COLUMN _cdc_lsn BIGINT")
    print("[OK] 已幫 silver.customers 加上 _cdc_lsn 欄位")
else:
    print("[SKIP] _cdc_lsn 欄位已存在")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 & 4: 讀取增量新事件、批次內去重並 MERGE INTO silver.customers
# MAGIC - 透過 `_silver_merge_checkpoint` 鎖定物理檔案進度，不漏掉任何慢車資料。
# MAGIC - 在 `whenMatched` 中進行 `s.lsn > t._cdc_lsn` 逐行對決，不同顧客互相隔離、絕不干擾。

# COMMAND ----------

def upsert_cdc_to_silver(micro_batch_df, batch_id):
    # 如果這 15 分鐘內沒有任何新的 CDC 事件檔案，直接跳過不耗費計算資源
    if micro_batch_df.isEmpty():
        print("[SKIP] 本次 15 分鐘內沒有新的 CDC 事件需要套用")
        return

    # 1. 批次內去重：在同一個 15 分鐘微批次內，針對同一個顧客按 lsn 與 ts_ms 降序排序
    w = Window.partitionBy(
        F.coalesce(F.get_json_object("after", "$.customer_id"), F.get_json_object("before", "$.customer_id"))
    ).orderBy(F.col("lsn").desc(), F.col("ts_ms").desc())

    dedup_events = (
        micro_batch_df
        .withColumn("customer_id", F.coalesce(
            F.get_json_object("after", "$.customer_id"),
            F.get_json_object("before", "$.customer_id")))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "customer_id", "op", "lsn",
            F.get_json_object("after", "$.first_name").alias("first_name"),
            F.get_json_object("after", "$.last_name").alias("last_name"),
            F.get_json_object("after", "$.email").alias("email"),
            F.get_json_object("after", "$.city").alias("city"),
            F.to_date(F.get_json_object("after", "$.signup_date")).alias("signup_date"),
            (F.get_json_object("after", "$.is_member") == "true").alias("is_member"),
            F.get_json_object("after", "$.age").cast("int").alias("age"),
        )
    )

    # 追蹤本次批次去重後的事件筆數
    pending_count = dedup_events.count()
    print(f"待套用事件數（去重後）: {pending_count}")

    # 2. 執行 MERGE INTO
    if pending_count > 0:
        target_table = DeltaTable.forName(spark, f"{catalog}.silver.customers")
        
        (target_table.alias("t")
         .merge(dedup_events.alias("s"), "t.customer_id = s.customer_id")
         .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t._cdc_lsn")
         .whenMatchedUpdate(
             condition="s.op IN ('c','u') AND s.lsn > t._cdc_lsn",
             set={
                 "first_name": "s.first_name",
                 "last_name": "s.last_name",
                 "email": "s.email",
                 "city": "s.city",
                 "signup_date": "s.signup_date",
                 "is_member": "s.is_member",
                 "age": "s.age",
                 "_cdc_lsn": "s.lsn",
                 "_updated_ts": "current_timestamp()",
             })
         .whenNotMatchedInsert(
             condition="s.op != 'd'",
             values={
                 "customer_id": "s.customer_id",
                 "first_name": "s.first_name",
                 "last_name": "s.last_name",
                 "email": "s.email",
                 "city": "s.city",
                 "signup_date": "s.signup_date",
                 "is_member": "s.is_member",
                 "age": "s.age",
                 "_cdc_lsn": "s.lsn",
                 "_updated_ts": "current_timestamp()",
             })
         .execute())
        print(f"[OK] 已套用 {pending_count} 筆 CDC 變更到 silver.customers")

# 3. 啟動增量管道：利用 Checkpoint 追蹤新檔案，availableNow 確保讀完這 15 分鐘資料就關機
silver_query = (spark.readStream
                .table(f"{catalog}.bronze.customers_cdc_log")
                .writeStream
                .format("delta")
                .option("checkpointLocation", f"{checkpoint_path}/_silver_merge_checkpoint")
                .foreachBatch(upsert_cdc_to_silver)
                .trigger(availableNow=True)
                .start())

silver_query.awaitTermination()
print("[OK] 本次 15 分鐘排程增量更新安全且冪等完成。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 檢查結果：套用後的 customers 表最新變動

# COMMAND ----------

display(spark.table(f"{catalog}.silver.customers").orderBy(F.desc("_updated_ts")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (可選) 用 Delta History 查每次 MERGE 的異動筆數，方便監控 CDC 套用狀況

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {catalog}.silver.customers LIMIT 10"))
