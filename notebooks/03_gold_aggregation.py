# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold 層：業務彙總表
# MAGIC 這一層產出可以直接給 BI 工具（如 Databricks SQL Dashboard、Power BI、Tableau）使用的彙總表：
# MAGIC - `daily_sales_summary`：每日營收、訂單數、客單價
# MAGIC - `customer_ltv`：顧客終身價值、下單次數、最近下單日
# MAGIC - `product_performance`：商品銷售額、銷量、毛利
# MAGIC - `channel_funnel`：各流量來源的瀏覽 -> 加入購物車 -> 結帳完成 轉換漏斗

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
catalog = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

orders = spark.table(f"{catalog}.silver.orders").filter("status = 'completed' AND is_current = true")
order_items = spark.table(f"{catalog}.silver.order_items")
customers = spark.table(f"{catalog}.silver.customers")
products = spark.table(f"{catalog}.silver.products").filter("is_current = true")
web_events = spark.table(f"{catalog}.silver.web_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: daily_sales_summary

# COMMAND ----------

daily_sales = (
    orders
    .withColumn("order_day", F.to_date("order_date"))
    .groupBy("order_day")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("order_total").alias("total_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value"),
    )
    .orderBy("order_day")
)

daily_sales.write.mode("overwrite").saveAsTable(f"{catalog}.gold.daily_sales_summary")
print(f"[OK] gold.daily_sales_summary: {daily_sales.count()} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: customer_ltv
# MAGIC 每位顧客的累積消費、訂單數、平均客單價、最近一次下單日、以及距今天數（可用來做流失風險分群）。

# COMMAND ----------

customer_orders = orders.groupBy("customer_id").agg(
    F.count("order_id").alias("order_count"),
    F.sum("order_total").alias("lifetime_value"),
    F.round(F.avg("order_total"), 2).alias("avg_order_value"),
    F.max("order_date").alias("last_order_date"),
)

customer_ltv = (
    customers.alias("c")
    .join(customer_orders.alias("o"), "customer_id", "left")
    .select(
        "c.customer_id", "c.first_name", "c.last_name", "c.city", "c.is_member",
        F.coalesce("o.order_count", F.lit(0)).alias("order_count"),
        F.coalesce("o.lifetime_value", F.lit(0.0)).alias("lifetime_value"),
        "o.avg_order_value",
        "o.last_order_date",
    )
    .withColumn("days_since_last_order",
                F.datediff(F.current_date(), F.col("last_order_date")))
    .withColumn("customer_segment",
                F.when(F.col("order_count") == 0, "no_purchase")
                 .when(F.col("days_since_last_order") <= 30, "active")
                 .when(F.col("days_since_last_order") <= 90, "at_risk")
                 .otherwise("churned"))
)

customer_ltv.write.mode("overwrite").saveAsTable(f"{catalog}.gold.customer_ltv")
print(f"[OK] gold.customer_ltv: {customer_ltv.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: product_performance

# COMMAND ----------

items_with_orders = order_items.join(
    orders.select("order_id"), "order_id", "inner"  # 只算 completed 訂單的商品
)

product_performance = (
    items_with_orders
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("total_revenue"),
    )
    .join(products, "product_id", "left")
    .withColumn("total_cost", F.col("units_sold") * F.col("cost"))
    .withColumn("gross_profit", F.round(F.col("total_revenue") - F.col("total_cost"), 2))
    .withColumn("gross_margin_pct",
                F.round(F.col("gross_profit") / F.col("total_revenue") * 100, 1))
    .select("product_id", "product_name", "category", "units_sold",
            "total_revenue", "gross_profit", "gross_margin_pct")
    .orderBy(F.desc("total_revenue"))
)

product_performance.write.mode("overwrite").saveAsTable(f"{catalog}.gold.product_performance")
print(f"[OK] gold.product_performance: {product_performance.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: channel_funnel
# MAGIC 各流量來源在 `page_view -> add_to_cart -> checkout_start -> checkout_complete` 的轉換率。

# COMMAND ----------

funnel_counts = (
    web_events
    .groupBy("channel")
    .pivot("event_type", ["page_view", "add_to_cart", "checkout_start", "checkout_complete"])
    .count()
    .fillna(0)
)

channel_funnel = (
    funnel_counts
    .withColumn("view_to_cart_rate",
                F.round(F.col("add_to_cart") / F.col("page_view") * 100, 2))
    .withColumn("cart_to_checkout_rate",
                F.round(F.col("checkout_start") / F.col("add_to_cart") * 100, 2))
    .withColumn("checkout_completion_rate",
                F.round(F.col("checkout_complete") / F.col("checkout_start") * 100, 2))
)

channel_funnel.write.mode("overwrite").saveAsTable(f"{catalog}.gold.channel_funnel")
print(f"[OK] gold.channel_funnel: {channel_funnel.count()} channels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 快速檢視結果

# COMMAND ----------

display(spark.table(f"{catalog}.gold.daily_sales_summary").orderBy(F.desc("order_day")).limit(10))

# COMMAND ----------

display(spark.table(f"{catalog}.gold.customer_ltv").orderBy(F.desc("lifetime_value")).limit(10))

# COMMAND ----------

display(spark.table(f"{catalog}.gold.product_performance").limit(10))

# COMMAND ----------

display(spark.table(f"{catalog}.gold.channel_funnel"))
