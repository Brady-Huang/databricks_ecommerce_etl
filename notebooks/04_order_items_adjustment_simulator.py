# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - 模擬訂單明細調整事件流：order_items
# MAGIC 訂單明細代表已發生的交易，真實財務/電商系統中原始交易記錄通常不可竄改。
# MAGIC 因此這裡**不模擬對 order_items 的覆蓋式 CDC**，而是模擬「調整事件」——
# MAGIC 退貨、數量修正、金額修正——這些事件會被記錄到獨立的 `order_item_adjustments` 表，
# MAGIC 原始 `order_items` 保持不動。
# MAGIC
# MAGIC 事件類型：
# MAGIC - `return`：整筆或部分退貨
# MAGIC - `quantity_change`：數量修正（例如訂單處理錯誤）
# MAGIC - `price_correction`：價格/折扣修正

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_order_items_path", "/Volumes/ecommerce_demo/raw/cdc_landing/order_items", "調整事件落地路徑")
dbutils.widgets.text("num_change_order_items_events", "150", "本次模擬要產生的調整事件數量")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_order_items_path")
num_change_events = int(dbutils.widgets.get("num_change_order_items_events"))

dbutils.fs.mkdirs(cdc_landing_path)

# COMMAND ----------

import random
import time
import json
from datetime import datetime


random.seed(int(time.time()))

# 拿目前 silver.order_items 當作參考基準，取樣哪些明細要被調整
try:
    existing_items = (
        spark.table(f"{catalog}.silver.order_items")
        .select("order_item_id", "order_id", "quantity", "unit_price", "discount_pct", "line_total")
        .toPandas()
    )
except Exception:
    existing_items = spark.table(f"{catalog}.bronze.order_items").limit(0).toPandas()

ADJUSTMENT_TYPES = ["return", "quantity_change", "price_correction"]
RETURN_REASONS = ["customer_changed_mind", "defective_item", "wrong_item_shipped", "late_delivery"]

# COMMAND ----------

def make_adjustment(row, adjustment_type):
    """依照調整類型，組出一筆調整事件的內容"""
    if adjustment_type == "return":
        # 退貨：全部數量退回，金額全額退回
        return {
            "adjustment_type": "return",
            "quantity_delta": -int(row["quantity"]),
            "amount_delta": -float(row["line_total"]),
            "reason": random.choice(RETURN_REASONS),
        }
    elif adjustment_type == "quantity_change":
        # 數量修正：增減 1~2 件，金額按比例調整
        delta_qty = random.choice([-2, -1, 1, 2])
        unit_price = float(row["unit_price"]) * (1 - float(row["discount_pct"]))
        return {

            "adjustment_type": "quantity_change",
            "quantity_delta": delta_qty,
            "amount_delta": round(delta_qty * unit_price, 2),
            "reason": "order_processing_correction",
        }
    else:
        # 價格修正：金額調整 -20% ~ +10%（例如事後補償折扣）
        pct = random.uniform(-0.20, 0.10)
        amount_delta = round(float(row["line_total"]) * pct, 2)
        return {
            "adjustment_type": "price_correction",
            "quantity_delta": 0,
            "amount_delta": amount_delta,
            "reason": "price_adjustment_after_order",
        }

# COMMAND ----------

events = []
lsn_counter = int(time.time() * 1000)

if len(existing_items) > 0:
    sample_size = min(num_change_events, len(existing_items))
    sampled_items = existing_items.sample(sample_size)

    for _, row in sampled_items.iterrows():
        lsn_counter += random.randint(1, 5)
        ts_ms = int(time.time() * 1000)
        adjustment_type = random.choices(ADJUSTMENT_TYPES, weights=[0.3, 0.3, 0.4])[0]
        adj = make_adjustment(row, adjustment_type)

        event = {

            "adjustment_id": f"ADJ{lsn_counter}",
            "order_item_id": row["order_item_id"],
            "order_id": row["order_id"],
            **adj,
            "source": {"table": "order_items", "lsn": lsn_counter, "ts_ms": ts_ms},
        }
        events.append(event)

random.shuffle(events)

batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = f"{cdc_landing_path}/batch_{batch_ts}"

json_lines = [json.dumps(e) for e in events]
df_out = spark.createDataFrame([(line,) for line in json_lines], ["value"])
df_out.coalesce(1).write.mode("overwrite").text(out_path)

print(f"[OK] 產生 {len(events)} 筆訂單明細調整事件 -> {out_path}")
print("調整類型分佈:", {t: sum(1 for e in events if e["adjustment_type"] == t) for t in ADJUSTMENT_TYPES})