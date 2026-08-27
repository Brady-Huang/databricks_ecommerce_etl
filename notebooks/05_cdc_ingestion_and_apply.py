# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - CDC 套用：把 Postgres 的變更同步到 silver.customers
# MAGIC 這個 notebook 做兩件事：
# MAGIC 1. **Bronze**：用 Auto Loader 把 CDC JSON 事件增量讀進 `bronze.customers_cdc_log`（append-only，保留完整變更歷史，方便追溯/重播）
# MAGIC 2. **Apply**：對每個 `customer_id`，在**同一批次內先取 LSN 最大的那筆事件**（避免同批次多次變更互相打架），
# MAGIC    再用 `MERGE INTO` 套用到 `silver.customers`，並且只在**新事件的 LSN 大於已套用的 LSN** 時才更新，
# MAGIC    這樣就算 CDC 事件順序錯亂、或這個 job 重跑，都不會把舊資料蓋過新資料（idempotent）。
# MAGIC
# MAGIC 這是 Databricks 官方推薦的 CDC 套用模式（MERGE + 序號欄位防止 out-of-order/重複套用）。

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
           .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
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
              .withColumn("_source_file", F.input_file_name())
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
# MAGIC ## Step 3: 找出「還沒套用過」的新事件，並在批次內去重（每個顧客只留 LSN 最大那筆）

# COMMAND ----------

try:
    last_applied_lsn = (spark.table(f"{catalog}.silver.customers")
                         .agg(F.max("_cdc_lsn")).collect()[0][0]) or 0
except Exception:
    last_applied_lsn = 0

new_events = spark.table(f"{catalog}.bronze.customers_cdc_log").filter(F.col("lsn") > F.lit(last_applied_lsn))

w = Window.partitionBy(F.coalesce(
        F.get_json_object("after", "$.customer_id"),
        F.get_json_object("before", "$.customer_id"))
    ).orderBy(F.col("lsn").desc())

dedup_events = (
    new_events
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

pending_count = dedup_events.count()
print(f"待套用事件數（去重後）: {pending_count}，上次已套用的 LSN: {last_applied_lsn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: MERGE INTO silver.customers
# MAGIC - `op = 'd'` → 刪除
# MAGIC - `op` 為 `c`/`u` 且尚未存在 → 新增
# MAGIC - `op` 為 `c`/`u` 且已存在 → 更新（MERGE 條件已經保證只會處理 LSN 較新的事件）

# COMMAND ----------

if pending_count > 0:
    (DeltaTable.forName(spark, f"{catalog}.silver.customers").alias("t")
     .merge(dedup_events.alias("s"), "t.customer_id = s.customer_id")
     .whenMatchedDelete(condition="s.op = 'd'")
     .whenMatchedUpdate(
         condition="s.op IN ('c','u')",
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
else:
    print("[SKIP] 沒有新的 CDC 事件需要套用")

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
