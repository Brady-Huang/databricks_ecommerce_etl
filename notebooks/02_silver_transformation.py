# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver 層：清洗與資料品質保證
# MAGIC 這一層把 Bronze 的原始資料轉換成「可信任、乾淨、有明確 schema」的表，處理：
# MAGIC - 去除重複紀錄
# MAGIC - 型別轉換（string -> date/timestamp/decimal）
# MAGIC - Null / 髒值處理
# MAGIC - 參照完整性檢查（例如 order 的 customer_id 是否存在於 customers）
# MAGIC - 用 Delta Lake 的 **MERGE INTO** 做 upsert，支援重跑不會產生重複資料

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
catalog = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: customers
# MAGIC - 用 `signup_date` 去重，保留每個 `customer_id` 最新一筆
# MAGIC - Email 統一轉小寫、過濾明顯無效格式
# MAGIC - 建立目標表（若不存在）並用 MERGE 做 upsert

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.customers (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    city STRING,
    signup_date DATE,
    is_member BOOLEAN,
    age INT,
    _updated_ts TIMESTAMP
) USING DELTA
""")

bronze_customers = spark.table(f"{catalog}.bronze.customers")

w = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

silver_customers_df = (
    bronze_customers
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("email", F.when(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.col("email")))
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("age", F.col("age").cast("int"))
    .withColumn("_updated_ts", F.current_timestamp())
    .select("customer_id", "first_name", "last_name", "email", "city",
            "signup_date", "is_member", "age", "_updated_ts")
)

(DeltaTable.forName(spark, f"{catalog}.silver.customers").alias("t")
 .merge(silver_customers_df.alias("s"), "t.customer_id = s.customer_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

print(f"[OK] Silver customers merged: {silver_customers_df.count()} incoming rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: products

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.products (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    cost DECIMAL(10,2),
    list_price DECIMAL(10,2),
    is_active BOOLEAN,
    _updated_ts TIMESTAMP
) USING DELTA
""")

silver_products_df = (
    spark.table(f"{catalog}.bronze.products")
    .dropDuplicates(["product_id"])
    .withColumn("cost", F.col("cost").cast("decimal(10,2)"))
    .withColumn("list_price", F.col("list_price").cast("decimal(10,2)"))
    .withColumn("_updated_ts", F.current_timestamp())
    .select("product_id", "product_name", "category", "cost", "list_price", "is_active", "_updated_ts")
)

(DeltaTable.forName(spark, f"{catalog}.silver.products").alias("t")
 .merge(silver_products_df.alias("s"), "t.product_id = s.product_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

print(f"[OK] Silver products merged: {silver_products_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: orders
# MAGIC 這裡示範**參照完整性檢查**：把 `customer_id` 對不到 `silver.customers` 的訂單，
# MAGIC 標記為 `is_valid_customer = false` 並記錄到資料品質日誌表，而不是直接丟棄，
# MAGIC 這樣business還是能看到「有多少訂單資料異常」。

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.orders (
    order_id STRING NOT NULL,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    payment_method STRING,
    order_total DECIMAL(12,2),
    is_valid_customer BOOLEAN,
    _updated_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.dq_log (
    check_name STRING,
    table_name STRING,
    failed_record_count BIGINT,
    checked_ts TIMESTAMP
) USING DELTA
""")

valid_customer_ids = spark.table(f"{catalog}.silver.customers").select("customer_id").distinct()
bronze_orders = spark.table(f"{catalog}.bronze.orders").dropDuplicates(["order_id"])

silver_orders_df = (
    bronze_orders
    .join(valid_customer_ids.withColumnRenamed("customer_id", "_valid_cid"),
          bronze_orders.customer_id == F.col("_valid_cid"), "left")
    .withColumn("is_valid_customer", F.col("_valid_cid").isNotNull())
    .withColumn("order_date", F.to_timestamp("order_date"))
    .withColumn("order_total", F.col("order_total").cast("decimal(12,2)"))
    .withColumn("_updated_ts", F.current_timestamp())
    .select("order_id", "customer_id", "order_date", "status", "payment_method",
            "order_total", "is_valid_customer", "_updated_ts")
)

(DeltaTable.forName(spark, f"{catalog}.silver.orders").alias("t")
 .merge(silver_orders_df.alias("s"), "t.order_id = s.order_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

invalid_count = silver_orders_df.filter("is_valid_customer = false").count()
spark.createDataFrame(
    [("orders_customer_fk_check", "silver.orders", invalid_count)],
    ["check_name", "table_name", "failed_record_count"]
).withColumn("checked_ts", F.current_timestamp()) \
 .write.mode("append").saveAsTable(f"{catalog}.silver.dq_log")

print(f"[OK] Silver orders merged. Invalid customer_id count: {invalid_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: order_items
# MAGIC 額外做「金額一致性檢查」：`line_total` 是否等於 `quantity * unit_price * (1 - discount_pct)`，
# MAGIC 不一致的紀錄記到 dq_log，方便追蹤上游資料品質。

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.order_items (
    order_item_id STRING NOT NULL,
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_pct DECIMAL(4,2),
    line_total DECIMAL(12,2),
    _updated_ts TIMESTAMP
) USING DELTA
""")

silver_items_df = (
    spark.table(f"{catalog}.bronze.order_items")
    .dropDuplicates(["order_item_id"])
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .withColumn("discount_pct", F.col("discount_pct").cast("decimal(4,2)"))
    .withColumn("line_total", F.col("line_total").cast("decimal(12,2)"))
    .withColumn("_updated_ts", F.current_timestamp())
)

amount_mismatch = silver_items_df.filter(
    F.abs(F.col("line_total") -
          F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct"))) > 0.05
).count()

spark.createDataFrame(
    [("order_items_amount_check", "silver.order_items", amount_mismatch)],
    ["check_name", "table_name", "failed_record_count"]
).withColumn("checked_ts", F.current_timestamp()) \
 .write.mode("append").saveAsTable(f"{catalog}.silver.dq_log")

(DeltaTable.forName(spark, f"{catalog}.silver.order_items").alias("t")
 .merge(silver_items_df.select(
        "order_item_id", "order_id", "product_id", "quantity",
        "unit_price", "discount_pct", "line_total", "_updated_ts").alias("s"),
        "t.order_item_id = s.order_item_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

print(f"[OK] Silver order_items merged. Amount mismatch count: {amount_mismatch}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: web_events

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.web_events (
    event_id STRING NOT NULL,
    customer_id STRING,
    event_type STRING,
    channel STRING,
    device_type STRING,
    event_ts TIMESTAMP,
    _updated_ts TIMESTAMP
) USING DELTA
""")

silver_events_df = (
    spark.table(f"{catalog}.bronze.web_events")
    .dropDuplicates(["event_id"])
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("_updated_ts", F.current_timestamp())
    .select("event_id", "customer_id", "event_type", "channel", "device_type", "event_ts", "_updated_ts")
)

(DeltaTable.forName(spark, f"{catalog}.silver.web_events").alias("t")
 .merge(silver_events_df.alias("s"), "t.event_id = s.event_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

print(f"[OK] Silver web_events merged: {silver_events_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 資料品質總覽

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.silver.dq_log ORDER BY checked_ts DESC"))
