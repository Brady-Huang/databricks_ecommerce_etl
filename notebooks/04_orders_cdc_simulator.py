# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - 模擬 PostgreSQL CDC 事件流：orders
# MAGIC 模擬的來源表：`orders`（Postgres 裡的訂單主檔），事件類型：
# MAGIC - `c` (create)：新訂單建立
# MAGIC - `u` (update)：訂單狀態變更（待付款 → 已出貨 → 已完成，或取消/退款）
# MAGIC - `d` (delete)：訂單記錄被刪除（少見，僅示範用）
# MAGIC
# MAGIC `orders` 採用 SCD Type 2 策略：下游 `05_orders_cdc_apply` 會保留每次狀態變更的
# MAGIC 完整歷史版本，用來支援 SLA 分析、漏斗分析、出貨延遲診斷。

# COMMAND ----------
dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_orders_path", "/Volumes/ecommerce_demo/raw/cdc_landing/orders", "CDC Orders 事件落地路徑")
dbutils.widgets.text("num_change_orders_events", "300", "本次模擬要產生的 change event 數量")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_orders_path")
num_change_events = int(dbutils.widgets.get("num_change_orders_events"))

dbutils.fs.mkdirs(cdc_landing_path)

# COMMAND ----------

import random
import time
import json
from datetime import datetime

random.seed(int(time.time()))

# 拿目前 silver.orders 的 current 版本當作「Postgres 現況」的參考基準
# 注意：一定要過濾 is_current = true，否則會拿歷史版本當作異動起點，產生錯誤的狀態轉移
try:
    existing_orders = (
        spark.table(f"{catalog}.silver.orders")
        .filter("is_current = true")
        .select("order_id", "customer_id", "order_date", "status", "payment_method", "order_total")
        .toPandas()
    )
except Exception:
    existing_orders = spark.table(f"{catalog}.bronze.orders").limit(0).toPandas()

# 訂單狀態的合理轉移路徑，避免模擬出不合邏輯的狀態跳躍（例如已完成 -> 待付款）
STATUS_TRANSITIONS = {
    "pending": ["completed", "cancelled"],
    "completed": ["refunded"],
    "cancelled": [],
    "refunded": [],
}

# COMMAND ----------

def next_status(current_status):
    """依照合理的狀態機邏輯，決定下一個狀態；若已是終態則回傳 None 代表不產生此次異動"""
    options = STATUS_TRANSITIONS.get(current_status, [])
    if not options:
        return None
    return random.choice(options)

def make_after_image(row=None, new_id=None, new_status=None):
    if row is not None:
        return {
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "order_date": str(row["order_date"]),
            "status": new_status,
            "payment_method": row["payment_method"],
            "order_total": float(row["order_total"]),
        }
    else:
        return {
            "order_id": new_id,
            "customer_id": f"C{random.randint(0, 5000):06d}",
            "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "pending",
            "payment_method": random.choice(["credit_card", "line_pay", "apple_pay", "bank_transfer", "cod"]),
            "order_total": round(random.uniform(100, 5000), 2),
        }

# COMMAND ----------

events = []
lsn_counter = int(time.time() * 1000)

# 只從「還有下一個合法狀態」的訂單裡取樣，避免對終態訂單模擬異動
eligible_orders = existing_orders[existing_orders["status"].isin(STATUS_TRANSITIONS.keys())]
eligible_orders = eligible_orders[eligible_orders["status"] != "cancelled"]
eligible_orders = eligible_orders[eligible_orders["status"] != "refunded"]

for i in range(num_change_events):
    lsn_counter += random.randint(1, 5)
    ts_ms = int(time.time() * 1000)
    roll = random.random()

    if len(eligible_orders) > 0 and roll < 0.85:
        # ---- UPDATE：訂單狀態推進 ----
        before_row = eligible_orders.sample(1).iloc[0]
        new_status = next_status(before_row["status"])
        if new_status is None:
            continue
        after = make_after_image(row=before_row, new_status=new_status)
        event = {
            "op": "u",
            "before": {
                "order_id": before_row["order_id"],
                "status": before_row["status"],
            },
            "after": after,
            "source": {"table": "orders", "lsn": lsn_counter, "ts_ms": ts_ms},
        }
    else:
        # ---- CREATE：新訂單建立 ----
        new_id = f"O{9000000 + i:07d}"
        event = {
            "op": "c",
            "before": None,
            "after": make_after_image(new_id=new_id),
            "source": {"table": "orders", "lsn": lsn_counter, "ts_ms": ts_ms},
        }

    events.append(event)

# 刻意打亂順序，模擬 CDC 事件送達順序跟發生順序不一致
random.shuffle(events)

batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = f"{cdc_landing_path}/batch_{batch_ts}"

json_lines = [json.dumps(e) for e in events]
df_out = spark.createDataFrame([(line,) for line in json_lines], ["value"])
df_out.coalesce(1).write.mode("overwrite").text(out_path)

print(f"[OK] 產生 {len(events)} 筆模擬訂單 CDC 事件 -> {out_path}")
print("op 分佈:", {op: sum(1 for e in events if e["op"] == op) for op in ["c", "u", "d"]})

# COMMAND ----------

# MAGIC %md
# MAGIC 重複執行這個 notebook（排程頻率由 Job B 控制），就能持續模擬訂單狀態的演進。
# MAGIC 下一步：`05_orders_cdc_apply` 會讀取這些事件，並用 SCD Type 2 的方式套用到 `silver.orders`。