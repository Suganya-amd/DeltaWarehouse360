# DeltaWarehouse360 🏗️

A production-grade **Medallion Architecture** ETL pipeline built on **Databricks**, **Delta Lake**, and **Unity Catalog**. It implements a complete Bronze → Silver → Gold data warehouse with **SCD Type 2** slowly changing dimension tracking, automated deployment via **Databricks Asset Bundles (DAB)**, and scheduled daily execution.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
  - [Bronze Layer](#bronze-layer---raw-ingestion)
  - [Silver Layer](#silver-layer---conformed--scd2)
  - [Gold Layer](#gold-layer---business-aggregations)
- [SCD Type 2 Implementation](#scd-type-2-implementation)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Local Setup](#local-setup)
  - [Deploy to Databricks](#deploy-to-databricks)
  - [Run the Pipeline](#run-the-pipeline)
- [Pipeline DAG](#pipeline-dag)
- [Testing](#testing)
- [Configuration](#configuration)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DeltaWarehouse360 Pipeline                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   CSV Files              Bronze              Silver         Gold    │
│  (UC Volume)          (Raw Delta)        (Conformed)    (Analytics) │
│                                                                     │
│  customers.csv  ──►  bronze_customers ──► dim_customer ──► sales    │
│  products.csv   ──►  bronze_products  ──► dim_product     summary  │
│  transactions   ──►  bronze_trans     ──► fact_trans   ──► CLV/RFM │
│    .csv                                   dim_date     ──► product │
│                                                            perf    │
│                                                        ──► monthly │
│                                                            trends  │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│  │  Deploy  │───►│  Bronze  │───►│  Silver  │───►│   Gold   │      │
│  │   CSV    │    │ Ingest   │    │  SCD2    │    │  Agg     │      │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘      │
│                                                                     │
│  Infrastructure: Databricks Asset Bundles · Unity Catalog · Delta  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
DeltaWarehouse360/
├── databricks.yml                  # DAB bundle configuration (dev/prod targets)
├── pyproject.toml                  # Python project config, Ruff linter settings
├── Makefile                        # Build, test, validate, deploy shortcuts
│
├── notebooks/
│   ├── bronze_ingestion.py         # Bronze: CSV → Delta with metadata & checksums
│   ├── silver_scd2.py              # Silver: SCD2 dimensions + enriched fact table
│   ├── gold_aggregations.py        # Gold: business KPIs, CLV, product scorecard
│   └── utils/
│       └── deploy_to_volume.py     # Utility: upload CSV fixtures to UC Volume
│
├── src/python/DeltaWarehouse360/
│   ├── __init__.py                 # Package exports
│   ├── main.py                     # CLI entrypoint
│   └── scd2_processor.py           # Reusable SCD Type 2 engine
│
├── resources/
│   ├── lakeflow_etl_job.yml        # ETL job definition (Bronze → Silver → Gold)
│   └── deploy_source_csv.yml       # CSV deployment job definition
│
├── data/                           # Source CSV fixtures
│   ├── customers_dim.csv
│   ├── products_dim.csv
│   ├── transactions_fact.csv
│   └── date_dim.csv
│
└── tests/
    └── test_scd2_processor.py      # Unit tests for SCD2 processor
```

---

## Data Model

### Bronze Layer — Raw Ingestion

Raw CSV data is ingested as-is into Delta tables with audit metadata columns appended.

| Table | Source | Added Columns |
|---|---|---|
| `bronze_customers` | `customers_dim.csv` | `ingestion_timestamp`, `etl_batch_id`, `source_system`, `source_file`, `record_checksum` (SHA-256) |
| `bronze_products` | `products_dim.csv` | Same audit columns |
| `bronze_transactions` | `transactions_fact.csv` | Same audit columns |

**Key features:**
- Explicit schema enforcement (no `inferSchema`)
- Null-safe SHA-256 record checksums for deduplication
- Full `overwrite` mode with `mergeSchema` for schema evolution

### Silver Layer — Conformed + SCD2

The Silver layer applies business logic, builds conformed dimensions with **SCD Type 2** change tracking, and enriches facts with surrogate keys.

| Table | Type | Description |
|---|---|---|
| `dim_customer` | SCD2 Dimension | Customer attributes tracked over time. Columns: `customer_sk`, `customer_id`, `customer_name`, `customer_email`, `country`, `customer_segment`, `credit_tier`, `start_date`, `end_date`, `is_current`, `is_current_record` |
| `dim_product` | SCD2 Dimension | Product attributes tracked over time. Columns: `product_sk`, `product_id`, `product_name`, `price`, `cost`, `manufacturer`, `discontinued_flag`, `start_date`, `end_date`, `is_current` |
| `fact_transactions` | Fact Table | Enriched transactions with point-in-time `customer_sk` and `product_sk` lookups, derived `discount_amount`, `net_amount`, `total_with_tax`, and temporal keys (`transaction_date_key`, `transaction_year`, `transaction_month`, `transaction_quarter`) |
| `dim_date` | Static Dimension | Calendar dimension from 2020-01-01 to 2030-12-31 with day-of-week, fiscal year/quarter, and weekend flags |

### Gold Layer — Business Aggregations

Analytics-ready tables optimized for dashboards and BI consumption.

| Table | Description | Key Metrics |
|---|---|---|
| `gold_sales_summary` | Daily sales aggregated by segment, category, channel, and region | `order_count`, `gross_revenue`, `net_revenue`, `total_discounts`, `avg_order_value`, `unique_customers` |
| `gold_customer_lifetime_value` | Per-customer CLV with RFM segmentation | `total_net_revenue`, `total_orders`, `recency_score`, `frequency_score`, `monetary_score`, `rfm_segment` (Champions / Loyal / At Risk / New / Lost / Potential Loyalists) |
| `gold_product_performance` | Product-level scorecard with margin analysis | `total_units_sold`, `margin_pct`, `est_total_margin`, `revenue_rank_overall`, `revenue_rank_in_category`, `revenue_contribution_pct` |
| `gold_monthly_trends` | Month-over-month trend analysis | `net_revenue`, `revenue_mom_change`, `revenue_mom_pct`, `cumulative_revenue`, `orders_mom_change` |

---

## SCD Type 2 Implementation

The [`SCD2Processor`](src/python/DeltaWarehouse360/scd2_processor.py) class provides a reusable, production-grade engine for Slowly Changing Dimension Type 2 logic:

```
┌──────────────┐     ┌──────────────┐
│   Incoming   │     │   Existing   │
│   Records    │     │  Dimension   │
└──────┬───────┘     └──────┬───────┘
       │                     │
       └──────┬──────────────┘
              │
    ┌─────────▼──────────┐
    │   Full Outer Join   │
    │  on Business Keys   │
    └─────────┬──────────┘
              │
    ┌─────────┼───────────────────┐
    │         │                   │
    ▼         ▼                   ▼
Unchanged  Changed             New
Records    Records            Records
    │     ┌───┴───┐              │
    │     │       │              │
    │  Expire   Insert           │
    │  Old Row  New Row          │
    │  (is_current=0)  (is_current=1)
    │     │       │              │
    └─────┴───────┴──────────────┘
              │
    ┌─────────▼──────────┐
    │   Union All +       │
    │   Historical Rows   │
    └─────────┬──────────┘
              │
    ┌─────────▼──────────┐
    │  Surrogate Key      │
    │  Assignment          │
    │  (deterministic,     │
    │   offset-based)      │
    └────────────────────┘
```

**Key design decisions:**
- **Null-safe comparison** using PySpark's `eqNullSafe()` — avoids false-positive change detection when comparing `NULL` values
- **Deterministic surrogate keys** using `row_number()` + max existing SK offset — ensures repeatable keys across runs
- **Point-in-time fact enrichment** — surrogate key lookups match the dimension record that was active on the transaction date (`start_date <= txn_date <= end_date`)
- **Expiration dating** — expired records get `end_date = date_sub(new.start_date, 1)` to maintain a gapless timeline

---

## Getting Started

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (v1.20+)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- Python 3.10 – 3.12
- A Databricks workspace with Unity Catalog enabled

### Local Setup

```bash
# Clone the repository
git clone https://github.com/Suganya-amd/DeltaWarehouse360.git
cd DeltaWarehouse360/DeltaWarehouse360

# Install dependencies
uv sync --dev

# Run linter and formatter
uv run ruff check .
uv run ruff format .

# Run unit tests
PYTHONPATH=src/python uv run python -m unittest discover -s tests
```

### Deploy to Databricks

```bash
# Authenticate (one-time setup)
databricks configure --profile prod

# Validate the bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Upload source CSV data to Unity Catalog Volume
databricks bundle run deploy-csv-job --target dev
```

### Run the Pipeline

```bash
# Run the full ETL pipeline (Bronze → Silver → Gold)
databricks bundle run deltawarehouse360_etl --target dev

# Deploy to production
databricks bundle deploy --target prod
databricks bundle run deltawarehouse360_etl --target prod
```

---

## Pipeline DAG

The ETL pipeline is orchestrated as a Databricks Workflow with task dependencies:

```
┌────────────────────┐
│  bronze_ingestion   │  CSV → Bronze Delta tables
│  (no dependencies)  │  with audit metadata
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  silver_processing  │  SCD2 dimensions +
│  (depends: bronze)  │  enriched fact table
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│  gold_aggregations  │  Business KPIs, CLV/RFM,
│  (depends: silver)  │  product perf, trends
└────────────────────┘
```

**Schedule:** Daily at 2:00 AM UTC (configurable in [`lakeflow_etl_job.yml`](resources/lakeflow_etl_job.yml))

---

## Testing

Unit tests cover the core SCD2 logic:

```bash
PYTHONPATH=src/python python -m unittest discover -s tests -v
```

| Test | Validates |
|---|---|
| `test_initial_load` | First-time dimension load with no existing data |
| `test_incremental_load_with_changes` | SCD2 expiration of old records and insertion of new versions |
| `test_incremental_load_no_changes` | Unchanged records pass through without duplication |
| `test_surrogate_key_generation` | Deterministic, offset-based surrogate key assignment |
| `test_main_execution` | Package entrypoint runs without errors |

---

## Configuration

### Bundle Targets

| Target | Mode | Catalog | Schema | Description |
|---|---|---|---|---|
| `dev` | Development | `workspace` | `default` | Development deployment with schedule paused |
| `prod` | Production | `workspace` | `default` | Production deployment with daily 2 AM UTC schedule |

### Key Variables

Defined in [`databricks.yml`](databricks.yml):

| Variable | Description | Default |
|---|---|---|
| `catalog_name` | Unity Catalog catalog | `workspace` |
| `schema_name` | Unity Catalog schema | `default` |

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | Databricks Serverless |
| Storage | Delta Lake on Unity Catalog |
| Orchestration | Databricks Workflows (Lakeflow) |
| Deployment | Databricks Asset Bundles (DAB) |
| Language | Python 3.10+ / PySpark |
| Packaging | Hatchling (wheel) |
| Linting | Ruff |
| Package Manager | uv |

---

## License

This project is for educational and portfolio purposes.
