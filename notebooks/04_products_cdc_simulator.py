# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - 模擬 PostgreSQL CDC 事件流：products
# MAGIC 模擬的來源表：`products`（Postgres 裡的商品主檔），事件類型：
# MAGIC - `c` (create)：新商品上架
# MAGIC - `u` (update)：價格調整、分類異動、上下架狀態變更
# MAGIC
# MAGIC `products` 採用 SCD Type 2 策略：下游 `05_products_cdc_apply` 會保留每次價格/分類
# MAGIC 變更的完整歷史版本，避免用現在的價格回溯計算歷史訂單的營收/毛利時失真。

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_products_path", "/Volumes/ecommerce_demo/raw/cdc_landing/products", "CDC Products 事件落地路徑")
dbutils.widgets.text("num_change_products_events", "150", "本次模擬要產生的 change event 數量")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_products_path")
num_change_events = int(dbutils.widgets.get("num_change_products_events"))

dbutils.fs.mkdirs(cdc_landing_path)

# COMMAND ----------

import random
import time
import json
from datetime import datetime

random.seed(int(time.time()))

# 拿目前 silver.products 的 current 版本當作「Postgres 現況」的參考基準
# 一定要過濾 is_current = true，否則會拿歷史版本當作異動起點
try:
    existing_products = (
        spark.table(f"{catalog}.silver.products")
        .filter("is_current = true")
        .select("product_id", "product_name", "category", "cost", "list_price", "is_active")
        .toPandas()
    )
except Exception:
    existing_products = spark.table(f"{catalog}.bronze.products").limit(0).toPandas()

CATEGORIES = ["Electronics", "Home & Kitchen", "Fashion", "Beauty", "Sports",
              "Books", "Toys", "Grocery", "Pet Supplies", "Office"]

# COMMAND ----------

def make_after_image(row=None, new_id=None):
    if row is not None:
        # 模擬價格調整：漲價或降價 5%~20%，偶爾伴隨分類調整
        price_change_pct = random.uniform(-0.20, 0.20)
        new_price = round(float(row["list_price"]) * (1 + price_change_pct), 2)
        new_category = random.choice(CATEGORIES) if random.random() < 0.1 else row["category"]
        return {
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "category": new_category,
            "cost": float(row["cost"]),
            "list_price": new_price,
            "is_active": bool(row["is_active"]) if random.random() > 0.05 else not bool(row["is_active"]),
        }
    else:
        cost = round(random.uniform(5, 500), 2)
        margin = random.uniform(1.2, 3.0)
        return {
            "product_id": new_id,
            "product_name": f"Product {new_id}",
            "category": random.choice(CATEGORIES),
            "cost": cost,
            "list_price": round(cost * margin, 2),
            "is_active": True,
        }

# COMMAND ----------

events = []
lsn_counter = int(time.time() * 1000)

for i in range(num_change_events):
    lsn_counter += random.randint(1, 5)
    ts_ms = int(time.time() * 1000)
    roll = random.random()

    if len(existing_products) > 0 and roll < 0.85:
        # ---- UPDATE：價格/分類異動 ----
        before_row = existing_products.sample(1).iloc[0]
        after = make_after_image(row=before_row)
        event = {
            "op": "u",
            "before": {
                "product_id": before_row["product_id"],
                "list_price": float(before_row["list_price"]),
                "category": before_row["category"],
            },
            "after": after,
            "source": {"table": "products", "lsn": lsn_counter, "ts_ms": ts_ms},
        }
    else:
        # ---- CREATE：新商品上架 ----
        new_id = f"P{90000 + i:05d}"
        event = {
            "op": "c",
            "before": None,
            "after": make_after_image(new_id=new_id),
            "source": {"table": "products", "lsn": lsn_counter, "ts_ms": ts_ms},
        }

    events.append(event)

random.shuffle(events)

batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = f"{cdc_landing_path}/batch_{batch_ts}"

json_lines = [json.dumps(e) for e in events]
df_out = spark.createDataFrame([(line,) for line in json_lines], ["value"])
df_out.coalesce(1).write.mode("overwrite").text(out_path)

print(f"[OK] 產生 {len(events)} 筆模擬商品 CDC 事件 -> {out_path}")
print("op 分佈:", {op: sum(1 for e in events if e["op"] == op) for op in ["c", "u"]})