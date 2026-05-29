# VMI Analytics — Microsoft Fabric Lakehouse Migration

A self-initiated, production-grade data engineering project rebuilding Sazerac's 
Vendor Monitored Inventory analytics infrastructure from a fragmented ecosystem of 
Excel workbooks and Power Query into a governed Microsoft Fabric Lakehouse — 
designed, architected, and executed by a single VMI analyst within the first month 
of tenure.

**Scope:** 90+ Power Query transformations across 7+ Excel workbooks → governed 
Medallion Lakehouse with automated Python ETL pipelines, Delta table architecture, 
structured logging, and live Oracle ERP shortcut views.

**Validated by:** Director of Master Data and Performance Reporting within 30 days 
of project initiation.

**AI-augmented development**: This project demonstrates applied use of generative AI (Claude/ChatGPT) as a productivity multiplier across SQL development, Python pipeline construction, architecture documentation, and debugging workflows — accelerating delivery without sacrificing engineering rigor or exposing confidentiality..

---

## Business Problem

The VMI team delivers a 5-column inventory file to Atlas planning software every 
business day by 4:00 PM. The process spanning the entire operation:

- 7+ Excel workbooks, 90+ Power Query transformations, manual XLOOKUP formulas
- Legacy ODBC connections tied to a single analyst's machine — un-runnable by 
  most team members
- ~40,000 rows dropped daily due to unresolved product code mismatches with no 
  validation that those exclusions are intentional
- Zero documentation, zero audit trail, zero automated failure handling
- No governed data layer — every downstream analytics process rebuilds the same 
  fragile file-based connections independently

This project addresses both the immediate operational risk and the structural 
architecture gap underneath it.

---

## Architecture

### Medallion Lakehouse Design

```
Source Systems / Files / ERP SQL Endpoints
                ↓
    VMI_Analytics Lakehouse (SCO Workspace)
                ↓
    raw → stg → core → out
                ↓
    Atlas Upload / Power BI / Downstream Models
```

### Schema Inventory

| Schema | Purpose |
|--------|---------|
| `raw_` | Pipeline-ingested source files, partitioned by load_date |
| `stg_` | Standardized, validated, renamed — one table per source |
| `core_` | Unified inventory position, SKU crosswalk joined |
| `out_` | Atlas upload format — auditable, reproducible |
| `cfg_` | VMI-owned governed reference data |
| `scut` | Live shortcuts to external ERP/TMS sources |
| `log_` | Pipeline run logs — append-only |
| `sbx_` | Sandbox — dev mode writes here |

### Key Design Decisions

- **Fresh ERP data over snapshots** — `scut.vw_BuyList_DetailInfo` and 
  `scut.vw_Orders_Base` queried live at pipeline runtime. BuyList and order data 
  is already maintained by IT; introducing a copy creates freshness lag and drift 
  risk with no benefit for a daily 4PM process.
- **scut schema for live shortcuts** — naming makes the distinction explicit; 
  `spark.table("scut.vw_BuyList_DetailInfo")` immediately signals a live external 
  source vs. a governed Lakehouse table.
- **No business logic in shared utility functions** — Atlas-specific filters 
  (ORTP32 exclusions, ITMS35 status codes) belong in process-specific notebooks, 
  not in shared utils. Centralise only after multiple processes share the same logic.
- **cust_in_transit as a separate domain** — in-transit data from source files and 
  from SQL OOIT lives in its own domain, joined to `core.cust_inventory` in the 
  core layer. Separating domains allows all sources to consolidate cleanly 
  regardless of origin.
- **Delete before upload for file drops** — Fabric Lakehouse overwrite does not 
  reliably update modified timestamps, breaking freshness validation. Standard 
  practice: always delete existing file before uploading the new version.

---

## ERP Integration — scut Schema

### Shortcut Views Built and Validated

Two live shortcut views were built in the `scut` schema, replicating core ERP 
views against Oracle data via Fabric shortcuts. Row counts validated against 
ERP source exactly.

| View | Row Count | Purpose |
|------|-----------|---------|
| `scut.vw_BuyList_DetailInfo` | 946,539 | Denormalized BuyList detail — SKU crosswalk, customer, item, compliance |
| `scut.vw_Orders_Base` | 2,632,879 | Oracle ERP open orders with fulfill-line deduplication |

### vw_BuyList_DetailInfo

Replicates `dbo.vw_BuyList_DetailInfo` from the ERP Fabric Data Warehouse. 
Five base tables joined via scut schema shortcuts:

- `scut.Buylist_Item` — 535K BuyList item records
- `scut.Buylist_Header` — 2,123 BuyList header records
- `scut.ItemMaster` — 112K item records (97K Informatica MDM, 10K SYS21)
- `scut.Buylist_Customer` — 3,948 customer-BuyList relationships (source of row fan-out)
- `scut.CustomerMaster` — 37K customer records

**Computed join key:**
```sql
BuyListDetailKey = CONCAT(ItemCode, Customer, Company, DeliveryAddressCode, ShipPoint1)
```

### vw_Orders_Base

Replicates `dbo.vw_Orders_Base` with a `LatestFulfillLine` CTE that deduplicates 
fulfill lines using `ROW_NUMBER()` partitioned by source order + line number — 
making the view safe for quantity aggregation (unlike `vw_Orders`, which produces 
duplicates on raw LEFT JOIN).

- 12 base tables joined across Oracle SCM extract objects
- Draft and reference orders excluded via `HeaderStatusCode` filter
- `DeliveryAddressCode` extracted from `PARTYSITENUMBER` via `SUBSTRING`/
  `CHARINDEX` logic — middle segment of hyphen-delimited party site number
- **Critical usage note:** always filter `WHERE SKU != 'PALLETSLIPCHG'` for 
  quantity-based analysis

**Computed join key:**
```sql
OrdersBaseKey = CONCAT(SKU, CustomerNumber, Company, DeliveryAddressCode, OrganizationCode)
```

**Join to BuyList:**
```sql
scut.vw_BuyList_DetailInfo.BuyListDetailKey = scut.vw_Orders_Base.OrdersBaseKey
```

### Fabric Shortcut Limitation Identified

Discovered a non-obvious Fabric architectural constraint: shortcuts cannot point 
to SQL views — only to storage objects. Some ERP tables are themselves shortcuts 
(to SYS21, Atlas) and cannot be re-shortcutted. This finding prevented a 
significant downstream pipeline failure. Correct pattern established: shortcut 
to native base tables → create view in Lakehouse SQL analytics endpoint using 
`scut.` table references.

---

## ERP Data Architecture Documentation

Complete reference documentation produced for the Oracle ERP data layer, 
replacing tribal knowledge with a queryable, AI-accessible architecture reference.

### Source Lineage Diagrams (6 views, 300 DPI)

| Diagram | View | Rows |
|---------|------|------|
| 01_vw_BuyList_DetailInfo | BuyList detail — primary operational view | 942K |
| 02_vw_BuyList_DetailInfo_MDM | BuyList MDM source — preferred for strict active record alignment | 665K |
| 03_vw_SKUTransition_BuyList | Gap analysis — BuyLists missing transition SKU | 19,838 |
| 04_vw_SKUTransition_Item | Item-level transition identification | 251 |
| 05_vw_SKUTransition_ManualAdditon | Manual brand manager transitions (EffectiveDate ≥ 2025) | 11 |
| 06_vw_SKUTransition_OpenOrders | Comprehensive order + transition view (SYS21 + Oracle) | 52,520 |

**Color coding:** Purple = External source system · Orange = Integration/staging · 
Blue = Core fact table · Green parallelogram = View · Teal = Supporting view

### Schema Documented — 13 Core Objects

Complete column-level documentation, row counts, grain definitions, and confirmed 
join keys for:

- Buylist_Item, Buylist_Header, Buylist_Customer, ItemMaster, CustomerMaster
- SAZ_BUYLIST_TO_ITEM_STAGING_TBL, SAZ_BUYLIST_HEADER_STAGING_TBL
- S21_InventoryBalance, SYS21_InventoryBalance, SYS21_OpenOrders
- PIMPlanningDetails, tbl_demand_master, tbl_supply_master

### Key Data Quality Discovery

Identified that the team's primary BuyList view (`vw_BuyList_DetailInfo`) was 
returning 72,416 fewer rows than the MDM source — confirmed via systematic SQL 
comparison. Root cause: different customer join behavior (fan-out) between the 
ETL-backed view and the MDM-direct view, plus 785 records the ETL carries as 
active that MDM has soft-deleted. Previously undetected.

---

## Atlas Upload — Source Lineage Audit

Full reverse-engineering of the existing Atlas upload process across 114 source 
queries and files — the investigation that surfaced the need for the Fabric migration.

### Scale of Existing Process

- **101 unique source queries** across 7+ Excel workbooks
- **3 machine-specific ODBC connections** blocking unattended pipeline execution
- **~40,000 rows dropped daily** due to unresolved SKU crosswalk mismatches 
  with no logging or visibility
- **10 of 114 sources** already accessible via Fabric at time of audit

### Key Findings from M Code Audit

| Source | Finding |
|--------|---------|
| SGWS (12 locations) | Maps `MaxOrderQty` → "On Hand Loaded" — Atlas receiving max reorder quantities as inventory figures daily across highest-volume distributor |
| RNDC GA/NM | Commented out of Append1 — GA and NM inventory silently excluded from Atlas upload |
| Breakthru 852 | File structure changed since 2022 — NETTABLE column now present, original M code fails |
| Virginia Bailment | In-transit file has no 2026 data — every shipped order returns blank status, zero in-transit cases |
| West Virginia Bailment | No status filter — all orders regardless of status treated as in-transit |
| LDF | References Column6 that doesn't exist — throws error on every run |
| VMIBL (SKU crosswalk) | ROW_NUMBER() deduplication logic and Company = 'SA' hardcode identified for parameterization |
| On Order In Transit | Triple deduplication, orphaned SIRF32 column, connection to final upload unconfirmed |

### VMIBL — The Core SKU Crosswalk

The `VMIBL` query is the most critical component in the pipeline — every 
distributor SKU must match via `Key3` to produce a valid `CATN32`. Unmatched 
SKUs produce null `CATN32` and are silently dropped. This is the primary driver 
of the ~40K daily row exclusions.

```
Key3 = Customer + ' ' + CustomerDeliveryAddressCode + StateReferenceNumber
Key2 = Dpl Customer (key) + Customer Item Reference
Join: Key2 (Append1) = Key3 (VMIBL) → returns CATN32
```

---

## Python Pipeline — Source Notebook Template

A standardized, config-driven notebook template for ingesting all 90+ sources 
into the Lakehouse. Analysts fill a single configuration block — no business 
logic changes required across the remaining notebook sections.

### Pipeline Stages (per source notebook)

```
Section 2  → Configuration block (only section requiring source-specific changes)
Section 4  → Extract source (freshness validation before read)
Section 5  → Validate raw (column structure, row count, nulls)
Section 6  → Write raw_ Delta table (partitioned by load_date)
Section 7  → Transform & standardize (rename → cast → null handling → dedupe)
Section 8  → Validate staged (business key uniqueness, post-transform nulls)
Section 9  → Write stg_ Delta table
Section 10 → Log run status (log.pipeline_runs)
Section 11 → Exception handling (failure logging — every run produces a log entry)
```

### vmi_utils.py — Shared Utility Library

Single shared utility file imported by every notebook. Key functions:

| Function | Purpose |
|----------|---------|
| `log_run()` | Writes structured entry to `log.pipeline_runs` Delta table |
| `register_source()` | Upserts source metadata to `cfg.source_registry` |
| `check_freshness()` | Validates file modified timestamp — warn/fail thresholds |
| `check_columns()` | Validates expected and required column presence |
| `check_row_count()` | Validates row count within min/max bounds |
| `check_nulls()` | Validates no nulls on required fields |
| `add_audit_columns()` | Appends src_file, run_id, load_date, load_timestamp |
| `get_sanitized_column_names()` | Returns sanitized column names for config population |
| `log_message()` | Timestamped notebook output logging (INFO/WARN/ERROR) |

### First Source Onboarded — Johnson Brothers (Validated)

```python
COLUMN_RENAMES = {
    "location"            : "location",
    "item"                : "item",
    "maxorderqty"         : "max_order_qty",
    "onhand_qty"          : "on_hand_qty",
    "total_in_transit_qty": "in_transit_qty"
}
```

Pipeline run log (validated, running daily in production):

| Date | Status | Raw Rows | Stg Rows |
|------|--------|----------|----------|
| 2026-05-18 | SUCCESS | 4,451 | 4,448 |
| 2026-05-15 | SUCCESS | 4,448 | 4,445 |

---

## Domain Standards — cust_inventory

All stg notebooks in the `cust_inventory` domain rename source columns to 
these standard targets, enabling the core notebook to union all sources cleanly:

| Column | Represents | Required |
|--------|-----------|---------|
| `location` | Distributor location or warehouse code | Yes |
| `item` | Distributor-facing item / SKU code | Yes |
| `on_hand_qty` | On-hand inventory quantity | Yes |
| `in_transit_qty` | In-transit quantity | If available |
| `max_order_qty` | Maximum order quantity | If available |

### Core Layer Join Logic

```python
# stg → BuyList crosswalk
stg.item + stg.location → scut.vw_BuyList_DetailInfo
ON stg.item = StateReferenceNumber
AND stg.location = CONCAT(Customer, ' ', CustomerDeliveryAddressCode)
→ resolves CATN32 (ItemCode on BuyList)
```

---

## Repository Contents

```
VMI_Analytics_Fabric_Migration/
├── sql/
│   ├── Create_vw_BuyList_DetailInfo.sql     # scut schema view — 946K rows validated
│   └── Create_vw_Orders_Base.sql            # scut schema view — 2.6M rows validated
├── notebooks/
│   ├── nbook_template_source.ipynb          # Standardized raw→stg template
│   └── nbook_stg_cust_inventory_johnson_brothers.ipynb  # First validated source
├── utils/
│   └── vmi_utils.py                         # Shared utility library
├── docs/
│   ├── ERP_Data_Architecture_Technical_Documentation.pdf
│   ├── 01_vw_BuyList_DetailInfo.png         # Source lineage diagram
│   ├── 02_vw_BuyList_DetailInfo_MDM.png
│   ├── 03_vw_SKUTransition_BuyList.png
│   ├── 04_vw_SKUTransition_Item.png
│   ├── 05_vw_SKUTransition_ManualAdditon.png
│   └── 06_vw_SKUTransition_OpenOrders.png
└── lineage/
    └── Atlas_Upload_Data_Lineage.csv        # Full 114-query source audit
```

---

## Project Status

| Epic | Status |
|------|--------|
| Epic 1 — Platform Standards & Architecture | ✅ Complete |
| Epic 2 — Atlas Lineage & Source Reverse Engineering | ✅ Complete |
| Epic 3 — Core Fabric Infrastructure | 🔄 In Progress |
| Epic 4 — Atlas Source Migration (90+ sources) | 🔄 In Progress — JB validated |
| Epic 5 — Atlas Master Pipeline | ⏳ Pending |
| Epic 6 — Scheduling, QA & Documentation | ⏳ Pending |
| Epic 7 — Expansion Roadmap | ⏳ Future |

**Tech stack:** Microsoft Fabric · Python · PySpark · Delta Lake · SQL · 
Oracle ERP · Informatica MDM · System21
