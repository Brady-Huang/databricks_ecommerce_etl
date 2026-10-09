# E-commerce ETL Pipeline (Databricks Medallion Architecture)

An end-to-end ETL project that can be imported straight into a Databricks Workspace and run: starting from simulated e-commerce data, it goes through the **Bronze → Silver → Gold** three-layer architecture and finally produces summary tables ready for BI use.

## Architecture

```
[Batch Data Generator]          [PostgreSQL CDC Simulator]
      |                                  |
      v                                  v
Landing Zone (CSV)            CDC Landing (JSON, Debezium format)
      |                                  |
      v  Auto Loader                     v  Auto Loader
+----------------+            +--------------------------+
|  Bronze layer  |            | bronze.customers_cdc_log |
|  (initial batch)|           | (append-only change log) |
+----------------+            +--------------------------+
      |                                  |
      v  Clean / dedupe / DQ checks      v  Order by LSN + MERGE (idempotent)
+----------------------------------------------------------+
|  Silver layer: customers / products / orders /           |
|  order_items / web_events (+ dq_log)                     |
|  The customers table is maintained by both the batch     |
|  initial load and the continuous CDC increments          |
+----------------------------------------------------------+
      |
      +-----------------------------+
      v                             v
+------------------+     +------------------------------+
|  Gold layer      |     |  ml.churn_features           |
|  daily_sales /   |     |  RFM + behavioral features + |
|  customer_ltv /  |     |  churn_label                 |
|  product_perf /  |     |  (feature engineering only,  |
|  channel_funnel  |     |   no model training)         |
+------------------+     +------------------------------+
```

### Why does `customers` use both batch loading and CDC?

This is a very common hybrid pattern in industry: at initial go-live, a one-time batch backfill of historical data is performed (00 → 01 → 02). After that, ongoing changes in Postgres (address updates, membership status changes, new sign-ups, account deletions) are synced in near real time via CDC (04 → 05). Both paths write back to the same `silver.customers` table.

## Data Model

| Table         | Description                                                   |
| ------------- | ------------------------------------------------------------- |
| `customers`   | Customer master (membership status, city, age)                |
| `products`    | Product master (cost, price, category)                        |
| `orders`      | Order header (status, payment method, order total)            |
| `order_items` | Order line items (quantity, discount, subtotal)               |
| `web_events`  | Website clickstream (views, add-to-cart, checkout events)     |

The data generator **intentionally injects some dirty data** (duplicate customers, invalid emails, unmatched customer_ids, records whose amounts don't match their line items) so that the cleaning and data quality check logic in the Silver layer has something meaningful to demonstrate.

## File Structure

```
databricks_ecommerce_etl/
├── README.md
├── notebooks/
│   ├── 00_generate_sample_data.py     # Generate simulated e-commerce data into the landing zone
│   ├── 01_bronze_ingestion.py         # Ingest into Bronze Delta tables via Auto Loader
│   ├── 02_silver_transformation.py    # Cleaning, dedup, DQ checks, MERGE upsert
│   ├── 03_gold_aggregation.py         # Build business summary tables
│   ├── 04_cdc_source_simulator.py     # Simulate Postgres CDC events (Debezium format)
│   ├── 05_cdc_ingestion_and_apply.py  # Auto Loader + MERGE to apply CDC to silver.customers
│   └── 06_churn_feature_table.py      # Churn prediction feature table (feature engineering only, no model training)
└── workflows/
    └── ecommerce_etl_job.json         # Databricks Jobs API definition (seven chained tasks)
```

Every `.py` file is in **Databricks Notebook source format** (it includes the `# Databricks notebook source` marker), so you can import it directly via Workspace → Import, or use the Databricks CLI:

```
databricks workspace import-dir ./notebooks /Workspace/ecommerce_etl/notebooks
```

## How to Run

### Option 1: Manual step-by-step execution (good for getting familiar with the flow)

1. Create a cluster in your Databricks Workspace (Runtime 15.4 LTS or later, with Unity Catalog).
2. Import and run the notebooks in order:
   - `00_generate_sample_data` → generate the data
   - `01_bronze_ingestion` → load into Bronze
   - `02_silver_transformation` → clean into Silver
   - `03_gold_aggregation` → produce the Gold summary tables
3. Each notebook takes its parameters through `dbutils.widgets`. The `catalog` defaults to `ecommerce_demo`; change it to match your Unity Catalog environment.

### Option 2: Scheduled execution with Databricks Workflows

1. Import `notebooks/` into the corresponding paths in your Workspace.
2. Create the Job from `workflows/ecommerce_etl_job.json`:

   ```
   databricks jobs create --json @workflows/ecommerce_etl_job.json
   ```

3. The Job includes a built-in daily schedule at 02:00 (Asia/Taipei), set to `PAUSED` by default. To enable it, change `schedule.pause_status` to `"UNPAUSED"`, or turn it on manually in the UI.

## PostgreSQL CDC Notes

`04_cdc_source_simulator.py` produces **simulated** Debezium-style events (because this environment is not connected to a real Postgres). The event format (`op` / `before` / `after` / `source.lsn`) matches real Debezium output, so:

- If you later want to connect a **real Postgres**, there are two options:
  1. **Recommended**: use the Postgres connector in Databricks **Lakeflow Connect** (fully managed, reads logical replication directly, no need to operate Debezium/Kafka yourself).
  2. **Self-managed**: have Debezium listen to the Postgres WAL → send to Kafka or sink directly to files → point the file path at the existing Auto Loader logic in `05_cdc_ingestion_and_apply.py`; nothing downstream needs to change.
- `05_cdc_ingestion_and_apply.py` uses the `_cdc_lsn` column to reject events that are older than the version already applied. Even if events arrive out of order or the job is re-run, no data will be overwritten by stale values (idempotent merge).

## Churn Feature Table Notes

`06_churn_feature_table.py` outputs `{catalog}.ml.churn_features`, which contains:

| Category           | Columns                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| Basic              | `tenure_days`, `city`, `is_member`                                                                                       |
| RFM                | `recency_days`, `frequency_lookback`, `monetary_lookback`, `avg_order_value_lookback`                                    |
| Purchase behavior  | `discount_usage_rate`, `distinct_categories_purchased`, `total_items_purchased`                                          |
| Web behavior       | `total_web_events_30d`, `distinct_event_types_30d`, `distinct_channels_30d`, `checkout_starts_30d`, `primary_device_30d` |
| Label              | `churn_label` (1 = no completed order within 90 days after `observation_date`)                                           |

`observation_date` is used to separate the feature computation window from the label window, avoiding the use of "future" information that would cause data leakage.
The notebook also contains commented-out Feature Engineering client registration code and sketch code for connecting AutoML later, but **no model training is actually executed**; it stops at a clean, ready-to-use feature table.

## Possible Extensions

- Rewrite Bronze/Silver/Gold with **Delta Live Tables (DLT)** to get automatic data quality monitoring (expectations) and lineage graphs
- Build a **Databricks SQL Dashboard** on top of the Gold layer for a live business dashboard
- Connect Lakeflow Connect or Debezium for real, replacing the simulated CDC with a real Postgres
- Hook `ml.churn_features` up to AutoML or a custom model to actually train and serve predictions