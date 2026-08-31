# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - 模擬 PostgreSQL CDC 事件流
# MAGIC 真實情境下，Postgres 的 CDC 通常長這樣：
# MAGIC - **推薦做法（生產環境）**：用 Databricks **Lakeflow Connect** 的 Postgres 連接器，
# MAGIC   它是全託管的，直接讀 Postgres 的邏輯複寫(logical replication)，不需要自己維護 Debezium/Kafka。
# MAGIC - **傳統做法（也很常見）**：Debezium 監聽 Postgres 的 WAL (Write-Ahead Log) →
# MAGIC   送到 Kafka 或直接 sink 成檔案（S3/ADLS/GCS）→ Databricks Auto Loader 讀取。
# MAGIC
# MAGIC 這個 notebook **沒有連線到真的 Postgres**，而是模擬 Debezium 送出來的 change event 格式
# MAGIC （`before` / `after` / `op` / `source.lsn`），讓你可以看到整條 CDC pipeline 的樣子，
# MAGIC 之後要接真的 Postgres，只要把這個 notebook 換成 Lakeflow Connect 或 Debezium 的輸出即可，
# MAGIC 下游 `05_cdc_ingestion_and_apply` 完全不用改。
# MAGIC
# MAGIC 模擬的來源表：`customers`（Postgres 裡的顧客主檔），事件類型：
# MAGIC - `c` (create)：新顧客註冊
# MAGIC - `u` (update)：改地址、改 email、會員狀態變更
# MAGIC - `d` (delete)：顧客要求刪除帳號 (GDPR/個資法場景)

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog Catalog 名稱")
dbutils.widgets.text("cdc_landing_path", "/Volumes/ecommerce_demo/raw/cdc_landing/customers", "CDC 事件落地路徑")
dbutils.widgets.text("num_change_events", "300", "本次模擬要產生的 change event 數量")

catalog = dbutils.widgets.get("catalog")
cdc_landing_path = dbutils.widgets.get("cdc_landing_path")
num_change_events = int(dbutils.widgets.get("num_change_events"))

dbutils.fs.mkdirs(cdc_landing_path)

# COMMAND ----------

import random
import time
import json
from datetime import datetime

random.seed(int(time.time()))  # 每次執行都要有新的變化，模擬持續發生的變更

# 拿目前 silver.customers 當作「Postgres 現況」的參考基準
try:
    existing_customers = (
        spark.table(f"{catalog}.silver.customers")
        .select("customer_id", "first_name", "last_name", "email", "city", "signup_date", "is_member", "age")
        .toPandas()
    )
except Exception:
    existing_customers = spark.table(f"{catalog}.bronze.customers").limit(0).toPandas()

CITIES = ["Taipei", "New Taipei", "Taichung", "Tainan", "Kaohsiung",
          "Hsinchu", "Keelung", "Chiayi", "Yilan", "Hualien"]

# COMMAND ----------

def make_after_image(row=None, new_id=None):
    """組出一筆 customer 的完整欄位（Debezium 的 after image 是變更後的完整 row）"""
    if row is not None:
        return {
            "customer_id": row["customer_id"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "email": row["email"],
            "city": random.choice(CITIES),  # 模擬搬家
            "signup_date": str(row["signup_date"]),
            "is_member": bool(random.random() < 0.5) if random.random() < 0.3 else bool(row["is_member"]),
            "age": int(row["age"]),
        }
    else:
        return {
            "customer_id": new_id,
            "first_name": f"FirstName{new_id}",
            "last_name": f"LastName{new_id}",
            "email": f"{new_id.lower()}@example.com",
            "city": random.choice(CITIES),
            "signup_date": datetime.now().strftime("%Y-%m-%d"),
            "is_member": random.random() < 0.35,
            "age": random.randint(18, 70),
        }

# COMMAND ----------

events = []
lsn_counter = int(time.time() * 1000)  # 模擬 Postgres 的 LSN(Log Sequence Number)，遞增代表事件發生順序

for i in range(num_change_events):
    lsn_counter += random.randint(1, 5)  # LSN 一定遞增，但事件送達下游的順序不保證(demo 下面會刻意打亂)
    ts_ms = int(time.time() * 1000)
    roll = random.random()

    if len(existing_customers) > 0 and roll < 0.55:
        # ---- UPDATE ----
        before_row = existing_customers.sample(1).iloc[0]
        after = make_after_image(row=before_row)
        event = {
            "op": "u",
            "before": {
                "customer_id": before_row["customer_id"],
                "city": before_row["city"],
                "is_member": bool(before_row["is_member"]),
            },
            "after": after,
            "source": {"table": "customers", "lsn": lsn_counter, "ts_ms": ts_ms},
        }
    elif len(existing_customers) > 0 and roll < 0.60:
        # ---- DELETE ----
        row = existing_customers.sample(1).iloc[0]
        event = {
            "op": "d",
            "before": {"customer_id": row["customer_id"]},
            "after": None,
            "source": {"table": "customers", "lsn": lsn_counter, "ts_ms": ts_ms},
        }
    else:
        # ---- CREATE (新顧客在 Postgres 上註冊) ----
        new_id = f"C{900000 + i}"
        event = {
            "op": "c",
            "before": None,
            "after": make_after_image(new_id=new_id),
            "source": {"table": "customers", "lsn": lsn_counter, "ts_ms": ts_ms},
        }

    events.append(event)

# 刻意打亂順序，模擬網路/多分區導致 CDC 事件「送達」的順序跟「發生」的順序不完全一致
# → 這正是下游一定要用 lsn 做排序、而不是用送達順序或 ingest 時間的原因
random.shuffle(events)

batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = f"{cdc_landing_path}/batch_{batch_ts}"

json_lines = [json.dumps(e) for e in events]
df_out = spark.createDataFrame([(line,) for line in json_lines], ["value"])
df_out.coalesce(1).write.mode("overwrite").text(out_path)

print(f"[OK] 產生 {len(events)} 筆模擬 CDC 事件 -> {out_path}")
print("op 分佈:", {op: sum(1 for e in events if e["op"] == op) for op in ["c", "u", "d"]})

# COMMAND ----------

# MAGIC %md
# MAGIC 重複執行這個 notebook（例如排程每 5 分鐘跑一次），就能持續模擬 Postgres 端不斷發生的變更。
# MAGIC 下一步：`05_cdc_ingestion_and_apply` 會讀取這些事件並套用到 `silver.customers`。
