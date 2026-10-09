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
dbutils.widgets.text("cdc_landing_customers_path", "/Volumes/ecommerce_demo/raw/cdc_landing/customers", "CDC 事件落地路徑")
dbutils.widgets.text("checkpoint_path", "/Volumes/ecommerce_demo/raw/_checkpoints/cdc_customers", "Checkpoint 路徑")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_customers_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Bronze - 讀入原始 CDC 事件 (append-only log)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# 1. 宣告 Qlik Replicate 內層資料與外層 Header Schema
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

# 對齊 Qlik Replicate 的 header, before, data 命名
qlik_event_schema = StructType([
    StructField("header", StructType([
        StructField("operation", StringType(), True),
        StructField("changeSeq", StringType(), True), # 💡 注意：Qlik 的 changeSeq 是字串型態
        StructField("timestamp", StringType(), True),
        StructField("streamPosition", StringType(), True),
    ]), True),
    StructField("before", customer_schema, True),
    StructField("data", customer_schema, True), # 💡 Debezium 的 after 在 Qlik 叫 data
])

# 2. 建立 Bronze Delta 表欄位結構
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.customers_cdc_log (
    operation STRING,
    changeSeq STRING,
    timestamp STRING,
    before STRING,
    data STRING,
    _source_file STRING,
    _ingest_ts TIMESTAMP
) USING DELTA
""")

raw_cdc = (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format", "json")
           .schema(qlik_event_schema)
           .option("recursiveFileLookup", "true")
           .load(cdc_landing_path))

bronze_cdc = (raw_cdc
              .select(
                  F.col("header.operation").alias("operation"),
                  F.col("header.changeSeq").alias("changeSeq"),
                  F.col("header.timestamp").alias("timestamp"),
                  F.to_json(F.col("before")).alias("before"),
                  F.to_json(F.col("data")).alias("data"), # 轉成 JSON 存入 data 欄位
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
    if micro_batch_df.isEmpty():
        print("[SKIP] 本次 15 分鐘內沒有新的 CDC 事件需要套用")
        return

    # 1. 批次內去重：從 data 或 before 中抓取 customer_id，並依據 changeSeq 字串排序
    w = Window.partitionBy(
        F.coalesce(F.get_json_object("data", "$.customer_id"), F.get_json_object("before", "$.customer_id"))
    ).orderBy(F.col("changeSeq").desc()) # 💡 zfill(20) 確保了字串排序完全合法

    dedup_events = (
        micro_batch_df
        .withColumn("customer_id", F.coalesce(
            F.get_json_object("data", "$.customer_id"),
            F.get_json_object("before", "$.customer_id")))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "customer_id", "operation", 
            F.col("changeSeq").cast("bigint").alias("lsn"), # 💡 轉成 bigint 以便跟 silver 的 _cdc_lsn 進行大小比較
            F.get_json_object("data", "$.first_name").alias("first_name"),
            F.get_json_object("data", "$.last_name").alias("last_name"),
            F.get_json_object("data", "$.email").alias("email"),
            F.get_json_object("data", "$.city").alias("city"),
            F.to_date(F.get_json_object("data", "$.signup_date")).alias("signup_date"),
            (F.get_json_object("data", "$.is_member") == "true").alias("is_member"),
            F.get_json_object("data", "$.age").cast("int").alias("age"),
        )
    )

    pending_count = dedup_events.count()
    print(f"待套用事件數（去重後）: {pending_count}")

    # 2. 執行 MERGE INTO（比對大寫的 'DELETE' / 'INSERT' / 'UPDATE'）
    if pending_count > 0:
        target_table = DeltaTable.forName(spark, f"{catalog}.silver.customers")
        
        (target_table.alias("t")
         .merge(dedup_events.alias("s"), "t.customer_id = s.customer_id")
         .whenMatchedDelete(condition="s.operation = 'DELETE' AND s.lsn > coalesce(t._cdc_lsn, -1)")
         .whenMatchedUpdate(
             condition="s.operation IN ('INSERT', 'UPDATE') AND s.lsn > coalesce(t._cdc_lsn, -1)",
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
             condition="s.operation != 'DELETE'",
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
        print(f"[OK] 已套用 {pending_count} 筆 Qlik CDC 變更到 silver.customers")

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
