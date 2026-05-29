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

**AI-augmented development:** This project demonstrates applied use of generative 
AI (Claude) as a productivity multiplier across SQL development, Python pipeline 
construction, architecture documentation, and debugging workflows — accelerating 
delivery without sacrificing engineering rigor.

---

## Repository Structure & Navigation

This repository is organized into three sections reflecting the three workstreams 
of the project. Each folder is self-contained with its own artifacts.

```
VMI_Analytics_FabricMigration/
│
├── FabricCode/                         ← Lakehouse pipeline code and SQL views
│   ├── Create_BuyListDetail_View.sql   — Live ERP shortcut view (946K rows validated)
│   ├── Create_OrdersBase_View.sql      — Live ERP orders view (2.6M rows validated)
│   ├── nbook_template_source.ipynb     — Standardized raw→stg ingestion template
│   ├── nbook_stg_cust_inventory_       — First validated production source notebook
│   │   johnson_brothers.ipynb
│   └── vmi_utils.py                    — Shared utility library (all notebooks)
│
├── ERP_Documentation/                  ← Oracle ERP architecture reference
│   ├── ERP_DataArchitecture_           — Full technical reference (13 objects,
│   │   TechnicalDocumentation.pdf        schema, join keys, row counts)
│   └── ERDs/                           — Source lineage diagrams (6 views, 300 DPI)
│       ├── 01_vw_BuyList_DetailInfo.png
│       ├── 02_vw_BuyList_DetailInfo_MDM.png
│       ├── 03_vw_SKUTransition_BuyList.png
│       ├── 04_vw_SKUTransition_Item.png
│       ├── 05_vw_SKUTransition_ManualAdditon.png
│       └── 06_vw_SKUTransition_OpenOrders.png
│
└── AtlasUpload/                        ← Current-state Atlas process audit
    ├── AtlasCurrentStateDataLineage.csv — Full 114-query source inventory
    └── CurrentStateQueryDocs/          — Reverse-engineered M code documentation
        ├── VMIBL_SQL_mcode.txt         — SKU crosswalk query (core of pipeline)
        ├── OnOrderInTransitData_SQL_   — In-transit SQL query documentation
        │   mcode.txt
        └── RNDC_GA_NM_mcode.txt        — Distributor source query documentation
```

**Where to start:**
- New to the project? → Read this README, then open `ERP_Documentation/ERP_DataArchitecture_TechnicalDocumentation.pdf`
- Reviewing the pipeline code? → Start with `FabricCode/nbook_template_source.ipynb`, then `vmi_utils.py`
- Reviewing the Atlas audit? → Open `AtlasUpload/AtlasCurrentStateDataLineage.csv`, then the `CurrentStateQueryDocs/` files

---

## Business Problem

The VMI team delivers a 5-column inventory file to Atlas planning software every 
business day by 4:00 PM. The existing process:

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
  belong in process-specific notebooks, not in shared utils. Centralise only 
  after multiple processes share the same logic.
- **cust_in_transit as a separate domain** — in-transit data from source files 
  and from SQL OOIT lives in its own domain, joined to `core.cust_inventory` in 
  the core layer.
- **Delete before upload for file drops** — Fabric Lakehouse overwrite does not 
  reliably update modified timestamps, breaking freshness validation.

---

## FabricCode — What's In Here

### SQL Views (`Create_BuyListDetail_View.sql`, `Create_OrdersBase_View.sql`)

Two live shortcut views built in the `scut` schema, replicating core ERP views 
against Oracle data via Fabric shortcuts. Row counts validated against ERP source 
exactly.

| View | Row Count | Purpose |
|------|-----------|---------|
| `scut.vw_BuyList_DetailInfo` | 946,539 | Denormalized BuyList — SKU crosswalk, customer, item, compliance |
| `scut.vw_Orders_Base` | 2,632,879 | Oracle ERP orders with fulfill-line deduplication CTE |

**Key architectural finding documented in the SQL:** Fabric shortcuts cannot point 
to SQL views — only storage objects. Some ERP tables are themselves shortcuts and 
cannot be re-shortcutted. Identifying this constraint prevented a significant 
downstream pipeline failure. The correct pattern: shortcut to native base tables 
→ create view in Lakehouse SQL analytics endpoint using `scut.` table references.

**Cross-view join key:**
```sql
scut.vw_BuyList_DetailInfo.BuyListDetailKey = scut.vw_Orders_Base.OrdersBaseKey
```

### Source Notebook Template (`nbook_template_source.ipynb`)

Standardized, config-driven notebook template for ingesting all 90+ sources. 
Analysts update one configuration block — no logic changes required elsewhere.

```
Section 2  → Configuration (only section requiring source-specific changes)
Section 4  → Extract source + freshness validation
Section 5  → Validate raw (columns, row count, nulls)
Section 6  → Write raw_ Delta table (partitioned by load_date)
Section 7  → Transform & standardize (rename → cast → null handling → dedupe)
Section 8  → Validate staged (business key uniqueness, post-transform nulls)
Section 9  → Write stg_ Delta table
Section 10 → Log run to log.pipeline_runs
Section 11 → Exception handling (every run produces a log entry)
```

### First Production Source (`nbook_stg_cust_inventory_johnson_brothers.ipynb`)

Template instantiated for Johnson Brothers — validated and running daily in production.

| Date | Status | Raw Rows | Stg Rows |
|------|--------|----------|----------|
| 2026-05-18 | SUCCESS | 4,451 | 4,448 |
| 2026-05-15 | SUCCESS | 4,448 | 4,445 |

### Shared Utility Library (`vmi_utils.py`)

Single file imported by every notebook. Updates here propagate automatically 
to all notebooks on next run.

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

---

## ERP_Documentation — What's In Here

### Technical Documentation PDF

Complete Oracle ERP data architecture reference — 13 core objects documented 
with column-level schema, row counts, grain definitions, and confirmed join keys. 
Produced to replace tribal knowledge with a queryable, AI-accessible reference.

Objects documented: Buylist_Item, Buylist_Header, Buylist_Customer, ItemMaster, 
CustomerMaster, SAZ_BUYLIST_TO_ITEM_STAGING_TBL, SAZ_BUYLIST_HEADER_STAGING_TBL, 
S21_InventoryBalance, SYS21_InventoryBalance, SYS21_OpenOrders, PIMPlanningDetails, 
tbl_demand_master, tbl_supply_master.

### ERDs — Source Lineage Diagrams

Six 300 DPI lineage diagrams tracing each ERP view from consumption layer back 
through all source systems. Color coded by layer:

| File | View | Rows |
|------|------|------|
| 01_vw_BuyList_DetailInfo | Primary BuyList operational view | 942K |
| 02_vw_BuyList_DetailInfo_MDM | MDM-direct view — preferred for strict active record alignment | 665K |
| 03_vw_SKUTransition_BuyList | Gap analysis — BuyLists missing transition SKU | 19,838 |
| 04_vw_SKUTransition_Item | Item-level transition identification | 251 |
| 05_vw_SKUTransition_ManualAdditon | Manual brand manager transitions (EffectiveDate ≥ 2025) | 11 |
| 06_vw_SKUTransition_OpenOrders | Comprehensive order + transition view (SYS21 + Oracle) | 52,520 |

**Color coding:** Purple = External source · Orange = Integration/staging · 
Blue = Core fact table · Green parallelogram = View · Teal = Supporting view

**Key data quality discovery documented here:** The team's primary BuyList view 
was returning 72,416 fewer rows than the MDM source — confirmed via systematic 
SQL comparison and elevated to the Director of Master Data. Previously undetected.

---

## AtlasUpload — What's In Here

### Data Lineage CSV (`AtlasCurrentStateDataLineage.csv`)

Full inventory of the existing Atlas upload process — 114 queries across 7+ 
Excel workbooks audited and documented. Columns include source file path, 
refresh frequency, delivery method, Fabric migration status, M code audit 
status, and remediation notes.

Key findings from the audit:
- 101 unique source queries identified
- 3 machine-specific ODBC connections blocking unattended execution
- ~40,000 rows dropped daily with no logging or visibility
- 10 of 114 sources already accessible via Fabric at time of audit

### Current State Query Documentation (`CurrentStateQueryDocs/`)

Three critical queries reverse-engineered and documented with full M code, 
transformation logic, column mapping, flags, and Python build notes:

**`VMIBL_SQL_mcode.txt`** — The SKU crosswalk. Most critical query in the 
pipeline — every distributor SKU must match here to produce a valid CATN32. 
Unmatched SKUs are silently dropped, making this the primary driver of the 
~40K daily row exclusions.

**`OnOrderInTransitData_SQL_mcode.txt`** — Oracle ERP in-transit query pulling 
orders within a 3-day window. Documents triple deduplication pattern, orphaned 
columns, and unconfirmed connection to final upload output.

**`RNDC_GA_NM_mcode.txt`** — Distributor source covering GA and NM markets. 
Documents that this query is currently commented out of the master append — 
meaning GA and NM inventory has been silently excluded from the Atlas upload.

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
