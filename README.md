# 電商 ETL Pipeline（Databricks Medallion Architecture）

一個可直接匯入 Databricks Workspace 執行的端對端 ETL 專案：從模擬電商資料開始，
走完 **Bronze → Silver → Gold** 三層架構，最後產出可供 BI 使用的彙總表。

## 架構圖

```
[批次資料產生器]              [PostgreSQL CDC 模擬器]
      |                              |
      v                              v
Landing Zone (CSV)          CDC Landing (JSON, Debezium 格式)
      |                              |
      v  Auto Loader                 v  Auto Loader
+----------------+          +------------------------+
|  Bronze 層     |          | bronze.customers_cdc_log|
|  (初始批次資料) |          | (append-only 變更日誌)  |
+----------------+          +------------------------+
      |                              |
      v  清洗/去重/DQ檢查             v  依 LSN 排序 + MERGE (idempotent)
+---------------------------------------------------+
|  Silver 層： customers / products / orders /       |
|  order_items / web_events (+ dq_log)               |
|  customers 表由「批次初始化」+「CDC 持續增量」共同維護|
+---------------------------------------------------+
      |
      +----------------------------+
      v                            v
+----------------+       +---------------------------+
|  Gold 層       |       |  ml.churn_features         |
|  daily_sales / |       |  RFM + 行為特徵 + churn_label|
|  customer_ltv /|       |  (特徵工程，不含模型訓練)    |
|  product_perf/ |       +---------------------------+
|  channel_funnel|
+----------------+
```

### 為什麼 `customers` 同時有批次載入跟 CDC？
這其實是業界很常見的混合模式：初次上線先做一次性的批次歷史資料回填（00→01→02），
之後 Postgres 端持續發生的變更（改地址、改會員狀態、新註冊、刪除帳號）則透過 CDC
以近即時的方式同步過來（04→05），兩條路徑最後都寫回同一張 `silver.customers`。

## 資料模型

| 表 | 說明 |
|---|---|
| `customers` | 顧客主檔（含會員狀態、城市、年齡） |
| `products` | 商品主檔（含成本、售價、分類） |
| `orders` | 訂單主檔（狀態、付款方式、訂單總額） |
| `order_items` | 訂單明細（數量、折扣、小計） |
| `web_events` | 網站點擊流（瀏覽、加入購物車、結帳事件） |

資料產生時**故意注入了一些髒資料**（重複顧客、無效 email、對不到的 customer_id、
金額與明細不一致的紀錄），這樣 Silver 層的清洗與資料品質檢查邏輯才有實際意義可以展示。

## 檔案結構

```
databricks_ecommerce_etl/
├── README.md
├── notebooks/
│   ├── 00_generate_sample_data.py     # 產生模擬電商資料，落地到 landing zone
│   ├── 01_bronze_ingestion.py         # Auto Loader 讀入 Bronze Delta 表
│   ├── 02_silver_transformation.py    # 清洗、去重、DQ 檢查、MERGE upsert
│   ├── 03_gold_aggregation.py         # 建立業務彙總表
│   ├── 04_cdc_source_simulator.py     # 模擬 Postgres CDC 事件 (Debezium 格式)
│   ├── 05_cdc_ingestion_and_apply.py  # Auto Loader + MERGE 套用 CDC 到 silver.customers
│   └── 06_churn_feature_table.py      # 流失預測特徵表 (只做特徵工程，不訓練模型)
└── workflows/
    └── ecommerce_etl_job.json         # Databricks Jobs API 定義（七個任務串接）
```

每個 `.py` 檔都是 **Databricks Notebook 原始格式**（含 `# Databricks notebook source`
標記），可以直接透過 Workspace → Import 匯入，或用 Databricks CLI：

```bash
databricks workspace import-dir ./notebooks /Workspace/ecommerce_etl/notebooks
```

## 如何執行

### 方式一：手動逐步執行（適合第一次熟悉流程）
1. 在 Databricks Workspace 建立一個 cluster（Runtime 15.4 LTS 以上，含 Unity Catalog）
2. 依序匯入並執行：
   - `00_generate_sample_data` → 產生資料
   - `01_bronze_ingestion` → 讀入 Bronze
   - `02_silver_transformation` → 清洗到 Silver
   - `03_gold_aggregation` → 產出 Gold 彙總表
3. 每個 notebook 都用 `dbutils.widgets` 帶參數，`catalog` 預設是 `ecommerce_demo`，
   可依你的 Unity Catalog 環境自行修改。

### 方式二：用 Databricks Workflow 排程執行
1. 把 `notebooks/` 匯入到 Workspace 對應路徑
2. 用 `workflows/ecommerce_etl_job.json` 建立 Job：
   ```bash
   databricks jobs create --json @workflows/ecommerce_etl_job.json
   ```
3. Job 內建每日 02:00（Asia/Taipei）排程，預設是 `PAUSED`，要啟用把
   `schedule.pause_status` 改成 `"UNPAUSED"`，或在 UI 上手動開啟。

## PostgreSQL CDC 說明

`04_cdc_source_simulator.py` 產生的是**模擬**的 Debezium 風格事件（因為這個環境沒有連線到真的 Postgres），
事件格式（`op` / `before` / `after` / `source.lsn`）跟真實 Debezium 輸出一致，所以：

- 如果之後要接**真的 Postgres**，有兩個選擇：
  1. **推薦**：用 Databricks **Lakeflow Connect** 的 Postgres 連接器（全託管，直接讀邏輯複寫，不用自己顧 Debezium/Kafka）
  2. **自建**：Debezium 監聽 Postgres WAL → 送到 Kafka 或直接 sink 成檔案 → 檔案路徑接到 `05_cdc_ingestion_and_apply.py` 現有的 Auto Loader 邏輯，下游完全不用改
- `05_cdc_ingestion_and_apply.py` 用 `_cdc_lsn` 欄位擋掉「比目前已套用版本舊」的事件，
  就算事件順序錯亂、或這個 job 重跑，都不會有資料被舊值蓋掉的問題（idempotent merge）

## 流失預測特徵表說明

`06_churn_feature_table.py` 輸出 `{catalog}.ml.churn_features`，包含：

| 類別 | 欄位 |
|---|---|
| 基本 | `tenure_days`、`city`、`is_member` |
| RFM | `recency_days`、`frequency_lookback`、`monetary_lookback`、`avg_order_value_lookback` |
| 消費行為 | `discount_usage_rate`、`distinct_categories_purchased`、`total_items_purchased` |
| 網站行為 | `total_web_events_30d`、`distinct_event_types_30d`、`distinct_channels_30d`、`checkout_starts_30d`、`primary_device_30d` |
| 標籤 | `churn_label`（1 = observation_date 後 90 天內沒有 completed 訂單） |

用 `observation_date` 把特徵計算跟標籤判定的時間窗切開，避免用到「未來」資訊造成資料洩漏。
notebook 裡也留了（註解掉的）Feature Engineering client 註冊程式碼，以及之後接 AutoML 的示意程式碼，
但目前**沒有實際執行訓練**，只到「乾淨可用的特徵表」為止，如你所說。

## 之後可以延伸的方向
- 用 **Delta Live Tables (DLT)** 重寫 Bronze/Silver/Gold，取得自動資料品質監控（expectations）與血緣圖
- 在 Gold 層之上接 **Databricks SQL Dashboard**，做成即時業務儀表板
- 真的接上 Lakeflow Connect 或 Debezium，把模擬 CDC 換成真實 Postgres
- 把 `ml.churn_features` 接上 AutoML 或自訂模型，真的訓練並上線推論

---
有任何一塊想要接著做（真的訓練模型、接 dashboard、換成 DLT），跟我說一聲。
