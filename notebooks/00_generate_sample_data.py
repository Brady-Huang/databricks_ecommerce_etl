# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Generate Simulated E-commerce Raw Data
# MAGIC This notebook generates four simulated e-commerce tables and lands them as CSV in a "landing zone" path,
# MAGIC mimicking how data arrives from upstream systems (order system, CRM) in a real-world scenario.
# MAGIC
# MAGIC Tables generated:
# MAGIC - `customers`: customer master
# MAGIC - `products`: product master
# MAGIC - `orders`: order header
# MAGIC - `order_items`: order line items
# MAGIC
# MAGIC The downstream pipeline (01/02/03) all starts reading from this landing zone, just as if it were connected to real source systems.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Unity Catalog catalog name")
dbutils.widgets.text("landing_volume_path", "", "Landing zone path (leave empty to derive from catalog)")
dbutils.widgets.text("num_customers", "5000", "Number of customers")
dbutils.widgets.text("num_products", "500", "Number of products")
dbutils.widgets.text("num_orders", "20000", "Number of orders")
dbutils.widgets.text("inject_dirty_data", "true", "Intentionally inject dirty data (to demonstrate Silver-layer cleaning)")
dbutils.widgets.text("force_regenerate", "false", "Generate even if the landing zone already has data")


catalog = dbutils.widgets.get("catalog")
landing_path = dbutils.widgets.get("landing_volume_path") or f"/Volumes/{catalog}/raw/landing"
num_customers = int(dbutils.widgets.get("num_customers"))
num_products = int(dbutils.widgets.get("num_products"))
num_orders = int(dbutils.widgets.get("num_orders"))
inject_dirty_data = dbutils.widgets.get("inject_dirty_data").lower() == "true"
force_regenerate = dbutils.widgets.get("force_regenerate").lower() == "true"
print(f"catalog={catalog}, landing_path={landing_path}")
print(f"customers={num_customers}, products={num_products}, orders={num_orders}")
print(f"inject_dirty_data={inject_dirty_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog / Schemas / Volumes (if they don't exist)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.cdc_landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw._checkpoints")

dbutils.fs.mkdirs(f"{landing_path}/customers")
dbutils.fs.mkdirs(f"{landing_path}/products")
dbutils.fs.mkdirs(f"{landing_path}/orders")
dbutils.fs.mkdirs(f"{landing_path}/order_items")

# ---------------------------------------------------------------------------
# Guard against duplicate writes
#
# Why: every run of this notebook writes a NEW timestamped batch folder. Running it
# twice would therefore stack two batches in the landing zone. Because the random
# seed is fixed, both batches contain the same IDs, and Auto Loader would ingest
# both, producing large-scale duplicates downstream.
#
# How: if any batch folder already exists, stop BEFORE generating anything, unless
# force_regenerate=true. Only the customers folder is checked, as a proxy for the
# whole landing zone.
#
# Order matters: this check must run AFTER the mkdirs above. On a fresh environment
# the folder then exists but is empty, so the check passes. Before the mkdirs,
# dbutils.fs.ls would fail because the path does not exist yet.
#
# To reset properly: drop the catalog (DROP CATALOG ... CASCADE) and re-run Job A,
# rather than setting force_regenerate=true.
#
# Limitation: this only protects the landing zone. It does not protect against the
# _cdc_lsn schema-evolution problem described in the README (Section 7, item 4).
# ---------------------------------------------------------------------------
existing_batches = [f.name for f in dbutils.fs.ls(f"{landing_path}/customers")]
if existing_batches and not force_regenerate:
    raise RuntimeError(
        f"Landing zone already contains data ({existing_batches[:3]} ...). "
        "Re-running 00 would stack duplicate batches. "
        "To reset, drop the catalog with DROP CASCADE, or set force_regenerate=true."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Logic
# MAGIC Uses `pandas` + `numpy` to generate data on the driver (suitable for demo-sized data).
# MAGIC If you need to scale up to tens of millions of rows, consider switching to distributed generation with Spark's `range()` + UDFs.

# COMMAND ----------

import numpy as np
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

CATEGORIES = ["Electronics", "Home & Kitchen", "Fashion", "Beauty", "Sports",
              "Books", "Toys", "Grocery", "Pet Supplies", "Office"]
CITIES = ["Taipei", "New Taipei", "Taichung", "Tainan", "Kaohsiung",
          "Hsinchu", "Keelung", "Chiayi", "Yilan", "Hualien"]

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
        # Intentionally inject some dirty data: empty emails, duplicate ids, inconsistent casing
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
        # Inject a few duplicate customers (simulating the upstream system re-sending records)
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
            # Occasionally apply a discount
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
            # Intentionally make a very small number of orders have a customer_id that doesn't exist
            # in the customers table (to demonstrate foreign-key dirty data)
            "customer_id": customer_id if not (inject_dirty_data and random.random() < 0.005)
                           else f"UNKNOWN_{uuid.uuid4().hex[:6]}",
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "order_total": round(order_total, 2),
        })

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data and Write to the Landing Zone

# COMMAND ----------

customers_pdf = gen_customers(num_customers)
products_pdf = gen_products(num_products)
orders_pdf, order_items_pdf = gen_orders_and_items(num_orders, customers_pdf, products_pdf)

datasets = {
    "customers": customers_pdf,
    "products": products_pdf,
    "orders": orders_pdf,
    "order_items": order_items_pdf,
}

for name, pdf in datasets.items():
    sdf = spark.createDataFrame(pdf)
    out_path = f"{landing_path}/{name}"
    # Land as CSV to mimic a file-based delivery from an upstream system;
    # a timestamped batch folder name simulates daily batch file drops
    batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (sdf.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{out_path}/batch_{batch_ts}"))
    print(f"[OK] {name}: {sdf.count()} rows -> {out_path}/batch_{batch_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC Data generation is complete. Next, run `01_bronze_ingestion` to load these raw files into the Bronze-layer Delta tables.

# COMMAND ----------