# Property Consumer Data Platform

A fully Dockerised learning platform for property data engineering, demonstrating:

- **Medallion Architecture** — Bronze → Silver (staging) → Gold (Data Vault 2.0) → Mart
- **Data Vault 2.0** — Hubs, Links, Satellites with SCD Type 2
- **PySpark ingestion** from CSV files and a simulated CRM database
- **dbt** transforms across all Medallion layers with built-in tests
- **Apache Airflow** DAG orchestrating the full daily pipeline
- **Apache Superset** dashboards reading from the Mart layer

## Architecture

```
scripts/generate_data.py   →  CSV + src_crm (PostgreSQL)
         ↓
spark/ingest.py (PySpark)  →  bronze schema (PostgreSQL)
         ↓
dbt staging models         →  silver schema  (stg_transactions, stg_properties, stg_owners, stg_agents)
         ↓
dbt vault models           →  gold schema    (hubs, links, satellites with SCD2)
         ↓
dbt mart models            →  mart schema    (property_sales, agent_performance, suburb_trends, owner_portfolio)
         ↓
Apache Superset            →  dashboards on mart schema
```

Orchestrated end-to-end by an **Airflow DAG** (`property_pipeline`) that runs daily.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- ~6 GB free disk space (images + data)
- Ports 5432, 8080, 8088 available on your host

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd dbt-practice
```

The `.env` file is included with development defaults — no changes needed to get started:

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=property_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
```

### 2. Build images

```bash
docker compose build
```

This builds three custom images: `spark-ingest`, `dbt`, and `airflow`.

### 3. Start core services

```bash
docker compose up -d postgres airflow superset
```

Wait ~30 seconds for Airflow and Superset to finish initialising.

**Service URLs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| PostgreSQL | localhost:5432 | admin / admin123 |

---

## Running the Pipeline

### Option A — Airflow (recommended)

The Airflow DAG `property_pipeline` runs the full pipeline end-to-end:

1. Open http://localhost:8080
2. Log in as `admin` / `admin`
3. Find the `property_pipeline` DAG and toggle it **on**
4. Click the ▶ (trigger) button to run it immediately

The DAG runs three tasks in sequence:

| Task | What it does |
|------|-------------|
| `generate_and_ingest` | Generates synthetic data with Faker and loads it into `bronze` via PySpark |
| `dbt_run` | Runs all dbt models: staging → vault hubs/links/satellites → mart |
| `dbt_test` | Runs all dbt data quality tests |

### Option B — Manual step-by-step

**Step 1 — Generate data and ingest into bronze**

```bash
# Start Postgres if not already running
docker compose up -d postgres

# Generate CSV data and seed src_crm tables
python scripts/generate_data.py --output-dir data

# Run PySpark ingestion into bronze schema
docker compose run --rm --profile ingest spark-ingest
```

**Step 2 — Run dbt transforms**

```bash
# Run all models (staging → vault → mart)
docker compose run --rm --profile dbt dbt dbt run

# Run data quality tests
docker compose run --rm --profile dbt dbt dbt test
```

**Step 3 — View dashboards**

Open http://localhost:8088 and log in as `admin` / `admin`.

---

## dbt Models

### Staging (Silver schema)

| Model | Source | Description |
|-------|--------|-------------|
| `stg_transactions` | `bronze.property_raw` | Transaction facts with hash keys and hashdiff |
| `stg_properties` | `bronze.property_raw` | Deduplicated property attributes |
| `stg_owners` | `bronze.src_crm_owners` | Owner data from CRM |
| `stg_agents` | `bronze.src_crm_agents` | Agent data from CRM |

### Data Vault 2.0 (Gold schema)

**Hubs** (business keys, insert-only):
`hub_property`, `hub_owner`, `hub_agent`, `hub_transaction`

**Links** (relationships between hubs, insert-only):
`lnk_transaction_property`, `lnk_transaction_buyer`, `lnk_transaction_seller`, `lnk_transaction_agent`

**Satellites** (attributes with SCD Type 2):
`sat_property_details`, `sat_owner_details`, `sat_agent_details`, `sat_transaction_details`

SCD2 is implemented via incremental materialization + a `post_hook` that sets `load_end_date` on the previous open row when a new version is inserted. Current records always have `load_end_date IS NULL`.

### Mart

| Model | Description |
|-------|-------------|
| `mart_property_sales` | One row per transaction joined to property and agent details |
| `mart_agent_performance` | Agent-level aggregates: volume, avg price, days on market |
| `mart_suburb_trends` | Monthly suburb-level median price and transaction count |
| `mart_owner_portfolio` | Per-owner buy/sell counts and net properties held |

---

## Running dbt Commands Individually

```bash
# Run only staging models
docker compose run --rm --profile dbt dbt dbt run --select staging

# Run only vault models
docker compose run --rm --profile dbt dbt dbt run --select vault

# Run only mart models
docker compose run --rm --profile dbt dbt dbt run --select mart

# Test a specific model
docker compose run --rm --profile dbt dbt dbt test --select mart_property_sales

# Check dbt connection to Postgres
docker compose run --rm --profile dbt dbt dbt debug
```

---

## Superset Dashboards

After the pipeline has run, Superset is pre-configured with a `Property DB` connection pointing at the `mart` schema.

To add datasets and build charts:

1. **Settings → Database Connections** — verify `Property DB` is listed
2. **Datasets → + Dataset** — add each of the four mart tables
3. **Charts → + Chart** — create charts from the datasets
4. **Dashboards → + Dashboard** — assemble charts into dashboards

Suggested dashboards:

| Dashboard | Dataset | Charts |
|-----------|---------|--------|
| Property Sales Overview | `mart_property_sales` | Sales volume over time, median price by suburb, sale type breakdown |
| Agent Performance | `mart_agent_performance` | Top agents by volume, avg days on market |
| Suburb Trends | `mart_suburb_trends` | Median price trend by suburb (line chart) |
| Owner Portfolio | `mart_owner_portfolio` | Investor vs owner split, net properties histogram |

---

## Project Structure

```
dbt-practice/
├── .env                          # environment variables (dev defaults)
├── docker-compose.yml            # all 5 services
├── postgres/
│   └── init.sql                  # creates schemas on first start
├── scripts/
│   ├── requirements.txt
│   └── generate_data.py          # Faker data generator (CSV + src_crm)
├── spark/
│   ├── Dockerfile
│   └── ingest.py                 # PySpark: CSV + src_crm → bronze
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/              # silver layer views
│       ├── vault/
│       │   ├── hubs/
│       │   ├── links/
│       │   └── satellites/       # SCD2 incremental tables
│       └── mart/                 # business-ready tables
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── property_pipeline.py  # ingest → dbt run → dbt test
├── superset/
│   └── init.sh                   # registers Property DB connection
└── tests/
    └── test_generate_data.py     # pytest unit tests for data generator
```

---

## Running Tests

Unit tests for the data generator:

```bash
pip install -r scripts/requirements.txt
pytest tests/test_generate_data.py -v
```

---

## Stopping Services

```bash
# Stop all running services
docker compose down

# Stop and remove all data (full reset)
docker compose down -v
```

After `down -v`, re-run from Step 3 of Quick Start to rebuild from scratch.
