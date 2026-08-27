# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - 流失預測特徵表（Feature Engineering Only）
# MAGIC 這個 notebook **不訓練模型**，只把「乾淨、可直接餵給模型」的特徵表準備好，
# MAGIC 之後不管是用 Databricks AutoML、自己寫 scikit-learn/XGBoost，都可以直接從
# MAGIC `{catalog}.ml.churn_features` 這張表開始。
# MAGIC
# MAGIC ## 關鍵設計：避免資料洩漏 (data leakage)
# MAGIC 流失預測最常犯的錯是「用了觀察當下還不存在的未來資訊」。這裡用兩個時間窗口把它切乾淨：
# MAGIC - **`observation_date`**：特徵計算的截止日，所有特徵只能用這天(含)之前的資料
# MAGIC - **`churn_window_days`**：往後看幾天沒下單就算流失，標籤用 `observation_date` 之後的資料算，
# MAGIC   跟特徵的時間窗完全分開
# MAGIC
# MAGIC 另外排除「太新的顧客」（signup 距離 observation_date 太近），因為他們還沒有足夠時間
# MAGIC 被觀察到是否會流失，硬塞進去只會讓標籤雜訊變大。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("observation_date", "2026-05-01", "特徵計算截止日 (YYYY-MM-DD)")
dbutils.widgets.text("churn_window_days", "90", "往後幾天沒下單算流失")
dbutils.widgets.text("min_tenure_days", "30", "顧客至少要註冊滿幾天才納入樣本")
dbutils.widgets.text("lookback_days", "180", "計算 RFM 等特徵時，往回看幾天的行為")

catalog = dbutils.widgets.get("catalog")
observation_date = dbutils.widgets.get("observation_date")
churn_window_days = int(dbutils.widgets.get("churn_window_days"))
min_tenure_days = int(dbutils.widgets.get("min_tenure_days"))
lookback_days = int(dbutils.widgets.get("lookback_days"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ml")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 讀取來源表，並依 `observation_date` 切出「特徵計算用」的資料範圍

# COMMAND ----------

customers = spark.table(f"{catalog}.silver.customers")
orders = spark.table(f"{catalog}.silver.orders")
order_items = spark.table(f"{catalog}.silver.order_items")
web_events = spark.table(f"{catalog}.silver.web_events")

obs_date = F.to_date(F.lit(observation_date))
lookback_start = F.date_sub(obs_date, lookback_days)
label_window_end = F.date_add(obs_date, churn_window_days)

# 特徵只能用 observation_date（含）之前的訂單/事件
orders_asof = orders.filter(F.to_date("order_date") <= obs_date)
items_asof = order_items.join(orders_asof.select("order_id"), "order_id", "inner")
events_asof = web_events.filter(F.to_date("event_ts") <= obs_date)

# 樣本母體：至少註冊滿 min_tenure_days 的顧客，才有意義去判斷是否流失
eligible_customers = customers.filter(
    F.datediff(obs_date, F.col("signup_date")) >= min_tenure_days
)

print(f"observation_date={observation_date}, churn_window_days={churn_window_days}")
print(f"符合樣本資格的顧客數: {eligible_customers.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM 與消費行為特徵（都用 observation_date 之前 `lookback_days` 天內的資料算）

# COMMAND ----------

recent_orders = orders_asof.filter(F.to_date("order_date") >= lookback_start).filter("status = 'completed'")

rfm = (
    recent_orders.groupBy("customer_id")
    .agg(
        F.datediff(obs_date, F.max(F.to_date("order_date"))).alias("recency_days"),
        F.count("order_id").alias("frequency_lookback"),
        F.sum("order_total").alias("monetary_lookback"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value_lookback"),
    )
)

# 折扣使用率、購買品類多樣性
items_recent = items_asof.join(
    recent_orders.select("order_id"), "order_id", "inner"
).join(spark.table(f"{catalog}.silver.products").select("product_id", "category"), "product_id", "left")

items_recent_with_customer = items_recent.join(
    recent_orders.select("order_id", "customer_id"), "order_id", "inner"
)

purchase_behavior = (
    items_recent_with_customer.groupBy("customer_id")
    .agg(
        F.round(F.avg((F.col("discount_pct") > 0).cast("int")), 2).alias("discount_usage_rate"),
        F.countDistinct("category").alias("distinct_categories_purchased"),
        F.sum("quantity").alias("total_items_purchased"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 網站行為特徵（近 30 天，觀察 engagement，跟下單頻率是互補訊號）

# COMMAND ----------

events_30d = events_asof.filter(F.to_date("event_ts") >= F.date_sub(obs_date, 30))

engagement = (
    events_30d.groupBy("customer_id")
    .agg(
        F.count("event_id").alias("total_web_events_30d"),
        F.countDistinct("event_type").alias("distinct_event_types_30d"),
        F.countDistinct("channel").alias("distinct_channels_30d"),
        F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias("checkout_starts_30d"),
    )
)

# 最常使用的裝置（近似眾數）
device_mode = (
    events_30d.groupBy("customer_id", "device_type")
    .count()
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.desc("count"))))
    .filter("rn = 1")
    .select("customer_id", F.col("device_type").alias("primary_device_30d"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 標籤：`churn_label`
# MAGIC 定義：在 `observation_date` 之後、`churn_window_days` 天內**完全沒有 completed 訂單** → `churn_label = 1`。
# MAGIC 這段完全發生在 `observation_date` 之後，跟上面的特徵時間窗不重疊，避免洩漏。

# COMMAND ----------

future_orders = orders.filter(
    (F.to_date("order_date") > obs_date) &
    (F.to_date("order_date") <= label_window_end) &
    (F.col("status") == "completed")
)

customers_with_future_order = future_orders.select("customer_id").distinct().withColumn("_has_future_order", F.lit(1))

labels = (
    eligible_customers.select("customer_id")
    .join(customers_with_future_order, "customer_id", "left")
    .withColumn("churn_label", F.when(F.col("_has_future_order").isNull(), 1).otherwise(0))
    .select("customer_id", "churn_label")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 組成最終特徵表

# COMMAND ----------

churn_features = (
    eligible_customers
    .withColumn("tenure_days", F.datediff(obs_date, F.col("signup_date")))
    .join(rfm, "customer_id", "left")
    .join(purchase_behavior, "customer_id", "left")
    .join(engagement, "customer_id", "left")
    .join(device_mode, "customer_id", "left")
    .join(labels, "customer_id", "left")
    .select(
        "customer_id",
        F.lit(observation_date).cast("date").alias("observation_date"),
        "tenure_days",
        "city",
        "is_member",
        F.coalesce("recency_days", F.lit(lookback_days)).alias("recency_days"),
        F.coalesce("frequency_lookback", F.lit(0)).alias("frequency_lookback"),
        F.coalesce("monetary_lookback", F.lit(0.0)).alias("monetary_lookback"),
        "avg_order_value_lookback",
        F.coalesce("discount_usage_rate", F.lit(0.0)).alias("discount_usage_rate"),
        F.coalesce("distinct_categories_purchased", F.lit(0)).alias("distinct_categories_purchased"),
        F.coalesce("total_items_purchased", F.lit(0)).alias("total_items_purchased"),
        F.coalesce("total_web_events_30d", F.lit(0)).alias("total_web_events_30d"),
        F.coalesce("distinct_event_types_30d", F.lit(0)).alias("distinct_event_types_30d"),
        F.coalesce("distinct_channels_30d", F.lit(0)).alias("distinct_channels_30d"),
        F.coalesce("checkout_starts_30d", F.lit(0)).alias("checkout_starts_30d"),
        "primary_device_30d",
        "churn_label",
    )
)

(churn_features.write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{catalog}.ml.churn_features"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 檢查：樣本數、標籤分佈、特徵缺值狀況

# COMMAND ----------

result = spark.table(f"{catalog}.ml.churn_features")
print(f"樣本數: {result.count()}")
display(result.groupBy("churn_label").count())

# COMMAND ----------

display(result.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (可選) 註冊成 Databricks Feature Engineering 的 Feature Table
# MAGIC 如果之後要用 Databricks AutoML 或做線上推論，建議用 Feature Engineering client
# MAGIC 把這張表登記成正式的 Feature Table，會拿到血緣(lineage)、線上服務(online store)等能力。
# MAGIC 這段預設沒有執行（避免在沒裝這個套件的 cluster 上噴錯），需要的話取消註解即可。

# COMMAND ----------

# from databricks.feature_engineering import FeatureEngineeringClient
#
# fe = FeatureEngineeringClient()
# fe.create_table(
#     name=f"{catalog}.ml.churn_features",
#     primary_keys=["customer_id", "observation_date"],
#     df=churn_features,
#     description="流失預測特徵表：RFM + 行為特徵 + churn_label，用 observation_date 切避免洩漏",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 之後要訓練模型時，大概長這樣（僅示意，這裡不執行）

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from databricks import automl
# MAGIC
# MAGIC train_df = spark.table(f"{catalog}.ml.churn_features").drop("customer_id", "observation_date")
# MAGIC summary = automl.classify(
# MAGIC     train_df,
# MAGIC     target_col="churn_label",
# MAGIC     primary_metric="f1",
# MAGIC     timeout_minutes=30,
# MAGIC )
# MAGIC ```
