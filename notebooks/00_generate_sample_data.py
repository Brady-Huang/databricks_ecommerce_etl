# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - 產生模擬電商原始資料
# MAGIC 這個 notebook 會產生五張模擬電商資料表，並以 CSV/JSON 落地到一個「landing zone」路徑，
# MAGIC 模擬真實情境中資料從上游系統（訂單系統、CRM、網站埋點）送進來的樣子。
# MAGIC
# MAGIC 產生的表：
# MAGIC - `customers`：顧客主檔
# MAGIC - `products`：商品主檔
# MAGIC - `orders`：訂單主檔
# MAGIC - `order_items`：訂單明細
# MAGIC - `web_events`：網站瀏覽/加入購物車事件（點擊流）
# MAGIC
# MAGIC 之後的 pipeline（01/02/03）都會從這個 landing zone 開始讀取，就像串接真實來源系統一樣。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 參數設定

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("landing_volume_path", "/Volumes/ecommerce_demo/raw/landing", "Landing Zone 路徑")
dbutils.widgets.text("num_customers", "5000", "顧客數量")
dbutils.widgets.text("num_products", "500", "商品數量")
dbutils.widgets.text("num_orders", "20000", "訂單數量")
dbutils.widgets.text("num_web_events", "80000", "網站事件數量")
dbutils.widgets.text("inject_dirty_data", "true", "是否故意注入髒資料(用來示範 Silver 層清洗)")

catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_volume_path")
num_customers = int(dbutils.widgets.get("num_customers"))
num_products = int(dbutils.widgets.get("num_products"))
num_orders = int(dbutils.widgets.get("num_orders"))
num_web_events = int(dbutils.widgets.get("num_web_events"))
inject_dirty_data = dbutils.widgets.get("inject_dirty_data").lower() == "true"

print(f"catalog={catalog}, landing_path={landing_path}")
print(f"customers={num_customers}, products={num_products}, orders={num_orders}, web_events={num_web_events}")
print(f"inject_dirty_data={inject_dirty_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 建立 Catalog / Schema / Volume（若不存在）

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing")

dbutils.fs.mkdirs(f"{landing_path}/customers")
dbutils.fs.mkdirs(f"{landing_path}/products")
dbutils.fs.mkdirs(f"{landing_path}/orders")
dbutils.fs.mkdirs(f"{landing_path}/order_items")
dbutils.fs.mkdirs(f"{landing_path}/web_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 資料產生邏輯
# MAGIC 用 `pandas` + `numpy` 在 driver 端產生資料（適合示範用的資料量）。
# MAGIC 若資料量要拉大到千萬筆等級，建議改成用 Spark 的 `range()` + UDF 分散式產生。

# COMMAND ----------

import numpy as np
import pandas as pd
import random
import string
import uuid
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

CATEGORIES = ["Electronics", "Home & Kitchen", "Fashion", "Beauty", "Sports",
              "Books", "Toys", "Grocery", "Pet Supplies", "Office"]
CITIES = ["Taipei", "New Taipei", "Taichung", "Tainan", "Kaohsiung",
          "Hsinchu", "Keelung", "Chiayi", "Yilan", "Hualien"]
CHANNELS = ["organic_search", "paid_ads", "social_media", "email", "direct", "referral"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["credit_card", "line_pay", "apple_pay", "bank_transfer", "cod"]
ORDER_STATUSES = ["completed", "completed", "completed", "cancelled", "refunded", "pending"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 8, 27)

# ---------- customers ----------
def gen_customers(n):
    rows = []
    for i in range(n):
        signup_date = random_date(START_DATE, END_DATE)
        email = f"user{i}@example.com"
        # 故意注入一些髒資料：空 email、重複 id、大小寫不一致
        if inject_dirty_data and random.random() < 0.01:
            email = None
        if inject_dirty_data and random.random() < 0.02:
            email = email.upper() if email else email
        rows.append({
            "customer_id": f"C{i:06d}",
            "first_name": f"FirstName{i}",
            "last_name": f"LastName{i}",
            "email": email,
            "city": random.choice(CITIES),
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "is_member": random.random() < 0.35,
            "age": int(np.clip(np.random.normal(35, 12), 18, 80)),
        })
    df = pd.DataFrame(rows)
    if inject_dirty_data:
        # 注入幾筆重複顧客 (模擬上游系統重送)
        dup = df.sample(frac=0.01, random_state=1)
        df = pd.concat([df, dup], ignore_index=True)
    return df

# ---------- products ----------
def gen_products(n):
    rows = []
    for i in range(n):
        cost = round(np.random.uniform(5, 500), 2)
        margin = np.random.uniform(1.2, 3.0)
        price = round(cost * margin, 2)
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": f"Product {i}",
            "category": random.choice(CATEGORIES),
            "cost": cost,
            "list_price": price,
            "is_active": random.random() < 0.95,
        })
    return pd.DataFrame(rows)

# ---------- orders + order_items ----------
def gen_orders_and_items(n_orders, customers_df, products_df):
    order_rows = []
    item_rows = []
    customer_ids = customers_df["customer_id"].tolist()
    product_records = products_df.to_dict("records")

    for i in range(n_orders):
        order_id = f"O{i:07d}"
        customer_id = random.choice(customer_ids)
        order_date = random_date(START_DATE, END_DATE)
        status = random.choice(ORDER_STATUSES)
        n_items = random.randint(1, 5)
        chosen_products = random.sample(product_records, k=min(n_items, len(product_records)))

        order_total = 0.0
        for item_idx, prod in enumerate(chosen_products):
            qty = random.randint(1, 3)
            unit_price = prod["list_price"]
            # 偶爾有折扣
            discount_pct = random.choice([0, 0, 0, 0.1, 0.2])
            line_total = round(qty * unit_price * (1 - discount_pct), 2)
            order_total += line_total
            item_rows.append({
                "order_item_id": f"{order_id}-{item_idx}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "line_total": line_total,
            })

        order_rows.append({
            "order_id": order_id,
            # 故意讓極少數訂單的 customer_id 對不到 customers 表 (示範 FK 髒資料)
            "customer_id": customer_id if not (inject_dirty_data and random.random() < 0.005)
                           else f"UNKNOWN_{uuid.uuid4().hex[:6]}",
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "order_total": round(order_total, 2),
        })

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)

# ---------- web_events ----------
def gen_web_events(n, customers_df):
    rows = []
    customer_ids = customers_df["customer_id"].tolist()
    event_types = ["page_view", "add_to_cart", "search", "checkout_start", "checkout_complete"]
    for i in range(n):
        rows.append({
            "event_id": str(uuid.uuid4()),
            "customer_id": random.choice(customer_ids) if random.random() > 0.1 else None,  # 訪客
            "event_type": random.choices(event_types, weights=[0.55, 0.2, 0.15, 0.06, 0.04])[0],
            "channel": random.choice(CHANNELS),
            "device_type": random.choice(DEVICE_TYPES),
            "event_ts": random_date(START_DATE, END_DATE).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 產生資料並寫入 Landing Zone

# COMMAND ----------

customers_pdf = gen_customers(num_customers)
products_pdf = gen_products(num_products)
orders_pdf, order_items_pdf = gen_orders_and_items(num_orders, customers_pdf, products_pdf)
web_events_pdf = gen_web_events(num_web_events, customers_pdf)

datasets = {
    "customers": customers_pdf,
    "products": products_pdf,
    "orders": orders_pdf,
    "order_items": order_items_pdf,
    "web_events": web_events_pdf,
}

for name, pdf in datasets.items():
    sdf = spark.createDataFrame(pdf)
    out_path = f"{landing_path}/{name}"
    # 用 CSV 落地，模擬上游系統常見的交付格式；帶時間戳記檔名模擬每日批次送檔
    batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (sdf.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{out_path}/batch_{batch_ts}"))
    print(f"[OK] {name}: {sdf.count()} rows -> {out_path}/batch_{batch_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC 資料已經產生完成。接下來執行 `01_bronze_ingestion` 把這些原始檔案讀進 Bronze 層 Delta 表。
