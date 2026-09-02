# Ecommerce ETL — Job 排程與 CDC 架構說明

## 專案定位

這個專案示範一套完整的 Medallion Architecture（Bronze → Silver → Gold）電商資料管線，核心亮點在於：**針對不同資料語意的維度表，採用對應的變更資料擷取（CDC）策略**，而不是所有表都套用同一種模式。這反映了資料工程中一個常被忽略、但實務上很重要的判斷：**同一種 CDC 手法，不代表適用所有資料表**。

---

## 一、資料表的 CDC 策略總覽

| 表 | 策略 | 商業邏輯 |
|---|---|---|
| **customers** | SCD Type 1 | 顧客資料（地址、會員狀態等）只在乎「現在」的狀態，異動時直接覆蓋，不保留歷史版本。 |
| **orders** | SCD Type 2 | 訂單狀態的變更歷史（待付款 → 已出貨 → 已完成）對 SLA 分析、漏斗分析、出貨延遲診斷有直接價值，因此保留每次狀態變更的完整時間軸。 |
| **products** | SCD Type 2 | 商品價格、分類會隨時間變動。若只保留最新價格，回溯計算歷史訂單的營收/毛利會失真，因此保留每次異動的歷史版本。 |
| **order_items** | **不做覆蓋式 CDC**，改用獨立的**調整事件表** | 訂單明細代表已發生的交易，真實財務/電商系統中原始交易記錄通常不可竄改。退貨、數量調整等異動不應直接 UPDATE 原始明細，而是另開一張 append-only 的事件表記錄異動，原始交易保持不動。 |
| **web_events** | 不處理 | 只攝取進 Bronze，不加工到 Silver/Gold。屬於行為事件流，目前沒有明確的下游業務需求，刻意排除在這次的範圍控制之外。 |

### 為什麼這樣分類：一致的判斷邏輯

> 凡是需要回溯歷史用於分析的維度（訂單狀態流程、商品價格），採用 SCD Type 2；只在乎當下結果的維度（顧客當前資料），採用 SCD Type 1；代表已完成交易、不應被竄改的紀錄（訂單明細），則完全跳出 SCD 框架，改用獨立的事件表模式。

這個設計選擇本身，展示了對「維度資料」與「交易記錄」該採用不同治理策略的判斷力，而不是機械式地對每張表套用同一種 CDC 樣板。

---

## 二、SCD Type 1 vs. Type 2 的實作差異

**SCD Type 1（customers）**

```sql
MERGE INTO silver.customers t USING dedup_events s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED AND s.op IN ('c','u') THEN UPDATE SET *, t._cdc_lsn = s.lsn
WHEN NOT MATCHED AND s.op != 'd' THEN INSERT *
```

單一 MERGE 完成，舊資料直接被覆蓋，不保留歷史。

**SCD Type 2（orders / products）**

每次異動需要兩個步驟：

1. **關閉舊版本**：找到目前 `is_current = true` 的那筆紀錄，將 `valid_to` 設為本次異動時間，`is_current` 設為 `false`
2. **開啟新版本**：插入一筆新紀錄，`valid_from` 設為本次異動時間，`valid_to` 設為遠期預設值，`is_current` 設為 `true`

對應的表結構會多出 `valid_from` / `valid_to` / `is_current` 三個欄位，同一個業務主鍵（`order_id` / `product_id`）會對應多筆歷史紀錄。

**order_items（獨立事件表模式）**

不修改原始 `silver.order_items`，改為新增 `silver.order_item_adjustments`：

```
adjustment_id STRING
order_item_id STRING
adjustment_type STRING   -- 'return' / 'quantity_change' / 'price_correction'
quantity_delta INT
amount_delta DECIMAL
reason STRING
adjusted_at TIMESTAMP
source_lsn BIGINT
```

這張表是 append-only，下游若要計算「某筆明細目前的真實數量/金額」，用原始 `order_items` LEFT JOIN 這張調整表做加總，而非直接修改原始資料。

---

## 三、Bronze CDC Log 設計：各表獨立，不合併

每張來源表對應一個獨立的 CDC log（`bronze.customers_cdc_log`、`bronze.orders_cdc_log`、`bronze.products_cdc_log`），而不是合併成一張統一的 log 表。

**理由：**

- 對應真實世界 Debezium「一個來源表對應一個 Kafka topic」的設計慣例
- 各表的 CDC pipeline 可以獨立監控、獨立除錯，互不影響
- 不同表的 `before`/`after` schema 差異大（尤其 SCD2 表需要額外欄位），硬塞進同一張表會讓 schema 設計變得尷尬

---

## 四、Job 排程架構

原本的設計把所有 notebook（含一次性的環境初始化腳本）串成一條每日排程的線性鏈路，這個設計混淆了三種不同性質、不同頻率的操作。拆分後的架構：

| Job | 內容 | 觸發方式 | 用途 |
|---|---|---|---|
| **A: `ecommerce_initial_snapshot`** | `00 → 01 → 02 → 03` | 手動觸發（PAUSED） | 環境初始化、或需要重建資料時執行。 |
| **B: `ecommerce_cdc_incremental`** | 四條並行的 CDC 支線（見下） | 每 15 分鐘 | 模擬近即時的資料異動同步，對應真實世界 Debezium/Lakeflow Connect 持續攝取 CDC 事件的頻率。 |
| **C: `ecommerce_daily_gold_refresh`** | `03 → 06` | 每天 02:00（Asia/Taipei） | 批次重算 BI 彙總表與流失特徵表，跟 CDC 頻率脫鉤，避免每 15 分鐘都重算一次高成本的聚合運算（freshness vs. cost 的取捨）。 |

> **架構調整說明**：`00_generate_sample_data` 原本設計為獨立腳本、不放入任何排程 Job，避免重複觸發時在 landing zone 疊加重複批次。實際部署後改為**納入 Job A 作為第一個 task**，理由是 `00` 內部的 `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` 語句是冪等的，配合 Job A「只在全新/剛清空的環境執行」這個使用前提，兩者是自洽的——**但這個前提必須被嚴格遵守**：Job A 不應該在資料已經被 Job B（CDC）動過的環境上重新觸發（見第七節「已知陷阱」）。如果需要重跑 Job A，正確做法是先把整個 catalog `DROP ... CASCADE` 清空（landing zone 的 Volume 也會隨之清空），而不是只重跑 Job 本身。

### Job B 內部的四條 CDC 支線

```
Job B: ecommerce_cdc_incremental（每 15 分鐘）
  ├── customers_cdc:    04_customers_cdc_simulator              → 05_customers_cdc_apply              (SCD1)
  ├── orders_cdc:       04_orders_cdc_simulator                 → 05_orders_cdc_apply                 (SCD2)
  ├── products_cdc:     04_products_cdc_simulator               → 05_products_cdc_apply               (SCD2)
  └── order_items_cdc:  04_order_items_adjustment_simulator     → 05_order_items_adjustment_apply      (獨立調整事件表，append-only)
```

四條支線彼此獨立，互不依賴，可以平行執行。Notebook 用表名直接命名（而非 `04a`/`04b` 這種字母後綴），可讀性更高，一看檔名就知道對應哪張表。

---

## 五、部署方式（Databricks Asset Bundles）

專案改用 **Databricks Asset Bundles（DAB）** 做基礎設施即程式碼（IaC）部署，取代早期用 CLI 逐一 `jobs create` 的手動方式：所有 Job 定義集中在根目錄的 `databricks.yml`，notebook 與 Job 設定一起納入版本控制，一個指令即可重現整套環境。

### 前置需求

- Databricks CLI（支援 `bundle` 指令的版本）
- 一組有效的 Databricks Personal Access Token

### 部署流程

```bash
# 1. 設定 CLI（僅需一次）
databricks configure --token

# 2. 驗證 bundle 設定
databricks bundle validate

# 3. 部署（把 notebooks/ 上傳到 workspace，並依 databricks.yml 建立三個 Job）
databricks bundle deploy -t dev

# 4. 依序執行、驗證
databricks bundle run job_a_initial_snapshot -t dev   # 初始化環境（僅在全新環境執行）
databricks bundle run job_b_cdc_incremental -t dev    # 驗證 CDC 邏輯
databricks bundle run job_c_daily_gold_refresh -t dev # 驗證 Gold 重算
```

首次部署時，`databricks.yml` 裡 Job B、Job C 的 `pause_status` 建議先設為 `PAUSED`，手動驗證流程無誤後，再改成 `UNPAUSED` 並重新 `deploy`，讓排程正式生效——避免驗證階段跟排程觸發時間點撞在一起（見第七節）。

---

## 六、與正式生產環境的差異（架構決策說明）

目前 `04` 開頭的四個 notebook 都是用程式碼模擬來源資料庫產生的 CDC 事件。若接上真實生產環境的 PostgreSQL，建議改用 **Databricks Lakeflow Connect 的 PostgreSQL 連接器**：

- 透過 PostgreSQL 原生的邏輯複製（logical replication）機制直接同步，不需要自建 Debezium/Kafka
- Lakeflow Connect 會自動處理「初始 Snapshot + 後續 Incremental CDC」的整合，追蹤複製進度，斷線後可從中斷點恢復
- 屆時 `04` 系列的模擬器 task 會被 Lakeflow Connect 的 ingestion pipeline 取代，`05` 系列的下游邏輯（排序、MERGE）基本不需要修改

**現實限制**：Lakeflow Connect 的 Postgres connector 目前仍是 Public Preview，需要向 Databricks 帳戶團隊申請資格，且 ingestion gateway 需要部署在你的 VPC 裡、與資料庫建立私有連線並列入防火牆白名單。這代表在 **Databricks Free Edition** 上無法實際落地真實 Postgres CDC——本專案的模擬器策略，是在此限制下驗證下游邏輯（idempotent merge、SCD1/2、獨立事件表）最務實的做法。

---

## 七、已知陷阱與除錯紀錄

實際開發與部署過程中踩過的坑，記錄下來避免重蹈覆轍：

1. **Widget 命名衝突**：`dbutils.widgets.text(name, default, label)` 只有在該 widget **尚不存在**時才會套用 `default` 值；若透過複製既有 notebook 建立新版本，且 widget 名稱沿用舊的（例如都叫 `cdc_landing_path`），可能會意外沿用舊 notebook 殘留在該 session 的 widget 值，導致資料被寫入錯誤路徑（products 的模擬事件曾被誤寫進 orders 的 landing 資料夾）。**解法**：每支 CDC notebook 的 widget 名稱都加上表名前綴（如 `cdc_landing_orders_path`、`cdc_landing_products_path`）。

2. **Volume 未預先建立**：`00_generate_sample_data` 早期版本只建立了 `landing` 這個 Volume，遺漏了 CDC 相關的 `_checkpoints` 與 `cdc_landing`，導致 `01_bronze_ingestion` 與 CDC 模擬器分別報 `UC_VOLUME_NOT_FOUND`。已修正為在 `00` 一次建立三個 Volume。

3. **Serverless environment 的 `client` 版本相容性**：Job 定義中 `environments.spec.client` 若設為較舊版本（如 `"1"`），部分較新開通的 workspace 會報 `Invalid platform channel Client-1`，需改用較新版本（`"3"`）。

4. **`_cdc_lsn` 欄位的 schema 演進問題**：`_cdc_lsn` 是 CDC apply（`05` 開頭）第一次執行時透過 `ADD COLUMNS` 自動加上去的。**如果先跑過 Job B（CDC），之後在同一份資料上重新執行 Job A（批次初始化），`02_silver_transformation` 的 MERGE 語句會因為批次來源資料沒有這個欄位而報 `DELTA_MERGE_UNRESOLVED_EXPRESSION`。** 這也是為什麼第四節強調 Job A 只能在全新環境執行——它本質上不是對已演進 schema 具備冪等性的操作，重跑前必須連同 catalog 一併清空重建。

5. **手動觸發與排程觸發時間點重疊**：若手動 `run-now`／`bundle run` 剛好撞上 Job B 的排程時間點（每 15 分鐘整），兩個 run 會同時對同一份 checkpoint／Silver 表寫入，其中一個會卡在等待鎖釋放（不是失敗，只是變慢，實測曾拖到 5-6 分鐘）。Delta Lake 的並行寫入機制保證資料一致性不會壞掉，但測試階段建議先將排程暫停（`pause_status: PAUSED`）再手動驗證，避免不必要的資源競爭。

---

## 八、已知簡化與未來改進方向

- **資料品質關卡**：目前 `dq_log` 只記錄檢查結果，尚未依照失敗筆數閾值中斷下游 task。下一步會在 Silver 與 Gold 之間插入品質檢查 task，失敗時阻斷下游執行並發送告警。
- **失敗告警**：三個 Job 尚未設定 `email_notifications` 或 Slack webhook，之後會補上。
- **多環境參數化**：目前透過 `databricks.yml` 的 `targets` 區分部署目標，尚未真正區分 dev/staging/prod 各自獨立的 catalog、compute 設定。
- **`_cdc_lsn` 的冪等性缺口**：`02_silver_transformation` 對 `_cdc_lsn` 欄位的處理，尚未讓批次初始化真正具備對「已演進 schema」的冪等性（見第七節第 4 點），是後續優先修正項目。
- **order_items 的調整事件表**（`silver.order_item_adjustments`）已完整實作並驗證，涵蓋 `return`/`quantity_change`/`price_correction` 三種調整類型，並示範了用原始 `order_items` LEFT JOIN 調整表計算「調整後真實金額」的查詢方式。真實系統中退貨流程通常還涉及庫存回補、退款狀態追蹤等下游流程，這裡先聚焦在資料模型層面的示範，未串接這些下游流程。
- **`03_gold_aggregation`** 讀取 `orders`/`products` 時已加上 `is_current = true` 過濾，避免 SCD2 的歷史版本重複被計入營收/銷量統計——這是 SCD2 表在下游查詢時最容易被忽略、卻也最容易出錯的地方，任何新增的下游查詢都必須記得加上這個過濾條件。
- **CI/CD**：尚未串接，未來可在 PR 合併時自動觸發 `databricks bundle validate` + `deploy`。
- **真實 Postgres CDC**：需升級到付費版並申請 Lakeflow Connect Preview 資格（見第六節）。