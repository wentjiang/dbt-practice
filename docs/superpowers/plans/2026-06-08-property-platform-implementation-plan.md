# Property Consumer Data Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fully dockerised dbt + Spark + Airflow + Superset learning platform for property consumer data using Data Vault 2.0, SCD Type 2, and Medallion Architecture.

**Architecture:** PySpark ingests CSV + PostgreSQL source data into a `bronze` schema; dbt-postgres runs all Medallion transforms (staging → Data Vault 2.0 hubs/links/satellites → mart); Airflow orchestrates the pipeline daily; Superset reads the `mart` schema for dashboards.

**Tech Stack:** Python 3.11, PySpark 3.5, dbt-core 1.8, dbt-postgres 1.8, Apache Airflow 2.9, PostgreSQL 15, Apache Superset 3.x, Docker Compose, Faker 24.x

**Spec:** `docs/superpowers/specs/2026-06-08-property-dbt-platform-design.md`

---

## File Map

```
dbt-practice/
├── .env                                      # environment variables
├── .gitignore
├── docker-compose.yml                        # all 5 services
├── scripts/
│   └── generate_data.py                      # Faker data generator (writes CSV + seeds src_crm)
├── spark/
│   ├── Dockerfile                            # bitnami/spark:3.5 + psycopg2
│   └── ingest.py                             # PySpark job: reads CSV + src_crm → bronze schema
├── dbt/
│   ├── Dockerfile                            # python:3.11-slim + dbt-postgres
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml                       # declares bronze schema sources
│       ├── staging/
│       │   ├── stg_transactions.sql
│       │   ├── stg_properties.sql
│       │   ├── stg_owners.sql
│       │   ├── stg_agents.sql
│       │   └── staging.yml                   # not_null / unique tests
│       ├── vault/
│       │   ├── hubs/
│       │   │   ├── hub_property.sql
│       │   │   ├── hub_owner.sql
│       │   │   ├── hub_agent.sql
│       │   │   └── hub_transaction.sql
│       │   ├── links/
│       │   │   ├── lnk_transaction_property.sql
│       │   │   ├── lnk_transaction_buyer.sql
│       │   │   ├── lnk_transaction_seller.sql
│       │   │   └── lnk_transaction_agent.sql
│       │   ├── satellites/
│       │   │   ├── sat_property_details.sql   # incremental + SCD2 post-hook
│       │   │   ├── sat_owner_details.sql
│       │   │   ├── sat_agent_details.sql
│       │   │   └── sat_transaction_details.sql
│       │   └── vault.yml                      # relationship tests
│       └── mart/
│           ├── mart_property_sales.sql
│           ├── mart_agent_performance.sql
│           ├── mart_suburb_trends.sql
│           ├── mart_owner_portfolio.sql
│           └── mart.yml
├── airflow/
│   ├── Dockerfile                            # apache/airflow:2.9 + docker provider
│   └── dags/
│       └── property_pipeline.py              # 3-task DAG
└── superset/
    └── init.sh                               # creates admin user + DB connection
```

---

## Phase 1 — Infrastructure

### Task 1: Project scaffold and Docker Compose

**Files:**
- Create: `.gitignore`
- Create: `.env`
- Create: `docker-compose.yml`

- [ ] **Step 1: Create `.gitignore`**

```
.env
__pycache__/
*.pyc
.venv/
data/
*.egg-info/
.superpowers/
```

- [ ] **Step 2: Create `.env`**

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=property_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123

AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://admin:admin123@postgres:5432/airflow_db
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=superset_secret_key_123

SUPERSET_SECRET_KEY=superset_secret_key_123
```

- [ ] **Step 3: Create `docker-compose.yml`**

```yaml
version: '3.8'

x-postgres-env: &postgres-env
  POSTGRES_HOST: ${POSTGRES_HOST}
  POSTGRES_PORT: ${POSTGRES_PORT}
  POSTGRES_DB: ${POSTGRES_DB}
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      retries: 10

  spark-ingest:
    build: ./spark
    environment:
      <<: *postgres-env
    volumes:
      - ./data:/data
      - ./spark/ingest.py:/app/ingest.py
    depends_on:
      postgres:
        condition: service_healthy
    command: ["spark-submit", "--packages", "org.postgresql:postgresql:42.7.1", "/app/ingest.py"]
    profiles: ["ingest"]

  dbt:
    build: ./dbt
    environment:
      <<: *postgres-env
    volumes:
      - ./dbt:/dbt
    working_dir: /dbt
    depends_on:
      postgres:
        condition: service_healthy
    profiles: ["dbt"]

  airflow:
    build: ./airflow
    environment:
      <<: *postgres-env
      AIRFLOW__CORE__EXECUTOR: ${AIRFLOW__CORE__EXECUTOR}
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: ${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW__CORE__FERNET_KEY}
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW__WEBSERVER__SECRET_KEY}
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      DOCKER_HOST: unix:///var/run/docker.sock
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
      - ./data:/data
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
    command: >
      bash -c "airflow db migrate &&
               airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
               airflow scheduler &
               airflow webserver"

  superset:
    image: apache/superset:3.1.0
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY}
    ports:
      - "8088:8088"
    volumes:
      - ./superset/init.sh:/app/init.sh
    depends_on:
      postgres:
        condition: service_healthy
    command: >
      bash -c "superset db upgrade &&
               superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin &&
               superset init &&
               /app/init.sh &&
               superset run -p 8088 --with-threads --reload --debugger"

volumes:
  postgres_data:
```

- [ ] **Step 4: Create Postgres init script `postgres/init.sql`**

```sql
CREATE DATABASE airflow_db;

\c property_db;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS src_crm;
```

- [ ] **Step 5: Verify compose parses cleanly**

```bash
docker compose config --quiet
```
Expected: no output (exit 0)

- [ ] **Step 6: Commit**

```bash
git add .gitignore .env docker-compose.yml postgres/
git commit -m "feat: docker compose scaffold with postgres, spark, dbt, airflow, superset"
```

---

## Phase 2 — Data Generation

### Task 2: Python data generator script

**Files:**
- Create: `scripts/requirements.txt`
- Create: `scripts/generate_data.py`
- Create: `tests/test_generate_data.py`

- [ ] **Step 1: Create `scripts/requirements.txt`**

```
faker==24.14.1
pandas==2.2.2
psycopg2-binary==2.9.9
```

- [ ] **Step 2: Install dependencies**

```bash
pip install -r scripts/requirements.txt
```

- [ ] **Step 3: Write failing tests**

Create `tests/test_generate_data.py`:

```python
import pandas as pd
import pytest
import sys
sys.path.insert(0, 'scripts')

from generate_data import generate_properties, generate_transactions, generate_owners, generate_agents

def test_generate_properties_returns_dataframe():
    df = generate_properties(n=10)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10

def test_generate_properties_columns():
    df = generate_properties(n=5)
    expected = {'property_id','address','suburb','postcode','state',
                'bedrooms','bathrooms','land_size_sqm','property_type',
                'year_built','listing_price'}
    assert expected.issubset(set(df.columns))

def test_generate_transactions_references_properties():
    props = generate_properties(n=20)
    owners = generate_owners(n=10)
    agents = generate_agents(n=5)
    txns = generate_transactions(n=50, properties=props, owners=owners, agents=agents)
    assert set(txns['property_id']).issubset(set(props['property_id']))
    assert set(txns['buyer_id']).issubset(set(owners['owner_id']))
    assert set(txns['agent_id']).issubset(set(agents['agent_id']))

def test_generate_transactions_columns():
    props = generate_properties(n=20)
    owners = generate_owners(n=10)
    agents = generate_agents(n=5)
    txns = generate_transactions(n=10, properties=props, owners=owners, agents=agents)
    expected = {'transaction_id','property_id','sale_price','sale_date',
                'buyer_id','seller_id','agent_id','sale_type',
                'settlement_date','days_on_market'}
    assert expected.issubset(set(txns.columns))

def test_raw_csv_has_all_columns():
    props = generate_properties(n=5)
    owners = generate_owners(n=5)
    agents = generate_agents(n=5)
    txns = generate_transactions(n=10, properties=props, owners=owners, agents=agents)
    # raw CSV merges transaction columns + property attribute columns
    raw = txns.merge(props, on='property_id', how='left')
    expected_tx = {'transaction_id','sale_price','sale_date','buyer_id','seller_id'}
    expected_prop = {'address','suburb','postcode','bedrooms','bathrooms'}
    assert expected_tx.issubset(set(raw.columns))
    assert expected_prop.issubset(set(raw.columns))

def test_generate_owners_columns():
    df = generate_owners(n=5)
    expected = {'owner_id','full_name','email','phone','suburb','state',
                'registration_date','investor_flag'}
    assert expected.issubset(set(df.columns))

def test_generate_agents_columns():
    df = generate_agents(n=5)
    expected = {'agent_id','full_name','email','agency_name','license_no',
                'region','accreditation_tier','active_from'}
    assert expected.issubset(set(df.columns))
```

- [ ] **Step 4: Run tests to verify they fail**

```bash
pytest tests/test_generate_data.py -v
```
Expected: `ModuleNotFoundError: No module named 'generate_data'`

- [ ] **Step 5: Implement `scripts/generate_data.py`**

```python
import uuid
import random
import pandas as pd
from faker import Faker
from faker.providers import address, internet, phone_number
import psycopg2
import os

fake = Faker('en_AU')
random.seed(42)
Faker.seed(42)

SUBURBS = ['Sydney','Melbourne','Brisbane','Perth','Adelaide','Canberra',
           'Hobart','Darwin','Geelong','Newcastle','Wollongong','Gold Coast',
           'Sunshine Coast','Cairns','Townsville','Bendigo','Ballarat']
STATES = {'Sydney':'NSW','Melbourne':'VIC','Brisbane':'QLD','Perth':'WA',
          'Adelaide':'SA','Canberra':'ACT','Hobart':'TAS','Darwin':'NT',
          'Geelong':'VIC','Newcastle':'NSW','Wollongong':'NSW',
          'Gold Coast':'QLD','Sunshine Coast':'QLD','Cairns':'QLD',
          'Townsville':'QLD','Bendigo':'VIC','Ballarat':'VIC'}
PROPERTY_TYPES = ['house','apartment','townhouse','unit','villa']
SALE_TYPES = ['auction','private_sale','expression_of_interest']
ACCREDITATION_TIERS = ['bronze','silver','gold','platinum']


def generate_properties(n: int = 2000) -> pd.DataFrame:
    records = []
    for _ in range(n):
        suburb = random.choice(SUBURBS)
        bedrooms = random.randint(1, 6)
        land = random.randint(80, 2000) if bedrooms >= 3 else random.randint(50, 200)
        records.append({
            'property_id': str(uuid.uuid4()),
            'address': fake.street_address(),
            'suburb': suburb,
            'postcode': fake.postcode(),
            'state': STATES[suburb],
            'bedrooms': bedrooms,
            'bathrooms': random.randint(1, 4),
            'land_size_sqm': land,
            'property_type': random.choice(PROPERTY_TYPES),
            'year_built': random.randint(1960, 2023),
            'listing_price': round(random.uniform(300_000, 3_000_000), -3),
        })
    return pd.DataFrame(records)


def generate_owners(n: int = 500) -> pd.DataFrame:
    records = []
    for _ in range(n):
        suburb = random.choice(SUBURBS)
        records.append({
            'owner_id': str(uuid.uuid4()),
            'full_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'suburb': suburb,
            'state': STATES[suburb],
            'registration_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
            'investor_flag': random.choice([True, False, False]),  # 33% investors
        })
    return pd.DataFrame(records)


def generate_agents(n: int = 100) -> pd.DataFrame:
    agencies = [fake.company() + ' Real Estate' for _ in range(20)]
    records = []
    for _ in range(n):
        suburb = random.choice(SUBURBS)
        records.append({
            'agent_id': str(uuid.uuid4()),
            'full_name': fake.name(),
            'email': fake.email(),
            'agency_name': random.choice(agencies),
            'license_no': f"LIC{random.randint(100000, 999999)}",
            'region': suburb,
            'accreditation_tier': random.choice(ACCREDITATION_TIERS),
            'active_from': fake.date_between(start_date='-15y', end_date='-1y').isoformat(),
        })
    return pd.DataFrame(records)


def generate_transactions(n: int, properties: pd.DataFrame,
                           owners: pd.DataFrame, agents: pd.DataFrame) -> pd.DataFrame:
    property_ids = properties['property_id'].tolist()
    owner_ids = owners['owner_id'].tolist()
    agent_ids = agents['agent_id'].tolist()
    records = []
    for _ in range(n):
        sale_date = fake.date_between(start_date='-5y', end_date='today')
        days_on_market = random.randint(3, 180)
        listing_price = float(properties.sample(1)['listing_price'].iloc[0])
        sale_price = listing_price * random.uniform(0.85, 1.15)
        buyer_id = random.choice(owner_ids)
        seller_id = random.choice([o for o in owner_ids if o != buyer_id])
        records.append({
            'transaction_id': str(uuid.uuid4()),
            'property_id': random.choice(property_ids),
            'sale_price': round(sale_price, 2),
            'sale_date': sale_date.isoformat(),
            'buyer_id': buyer_id,
            'seller_id': seller_id,
            'agent_id': random.choice(agent_ids),
            'sale_type': random.choice(SALE_TYPES),
            'settlement_date': fake.date_between(
                start_date=sale_date, end_date='+90d').isoformat(),
            'days_on_market': days_on_market,
        })
    return pd.DataFrame(records)


def build_raw_csv(transactions: pd.DataFrame, properties: pd.DataFrame) -> pd.DataFrame:
    return transactions.merge(properties, on='property_id', how='left')


def seed_src_crm(owners: pd.DataFrame, agents: pd.DataFrame, conn_params: dict) -> None:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    cur.execute("SET search_path TO src_crm")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS src_crm.owners (
            owner_id TEXT PRIMARY KEY,
            full_name TEXT, email TEXT, phone TEXT,
            suburb TEXT, state TEXT,
            registration_date DATE, investor_flag BOOLEAN
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS src_crm.agents (
            agent_id TEXT PRIMARY KEY,
            full_name TEXT, email TEXT, agency_name TEXT,
            license_no TEXT, region TEXT,
            accreditation_tier TEXT, active_from DATE
        )
    """)
    cur.execute("TRUNCATE src_crm.owners, src_crm.agents")
    for _, row in owners.iterrows():
        cur.execute("""
            INSERT INTO src_crm.owners VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s)
        """, tuple(row))
    for _, row in agents.iterrows():
        cur.execute("""
            INSERT INTO src_crm.agents VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s)
        """, tuple(row))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    import argparse, pathlib
    parser = argparse.ArgumentParser()
    parser.add_argument('--properties', type=int, default=2000)
    parser.add_argument('--transactions', type=int, default=10000)
    parser.add_argument('--owners', type=int, default=500)
    parser.add_argument('--agents', type=int, default=100)
    parser.add_argument('--output-dir', default='data')
    args = parser.parse_args()

    pathlib.Path(args.output_dir).mkdir(exist_ok=True)

    print("Generating properties...")
    props = generate_properties(args.properties)
    print("Generating owners...")
    owners = generate_owners(args.owners)
    print("Generating agents...")
    agents = generate_agents(args.agents)
    print("Generating transactions...")
    txns = generate_transactions(args.transactions, props, owners, agents)

    raw = build_raw_csv(txns, props)
    raw.to_csv(f'{args.output_dir}/property_raw.csv', index=False)
    print(f"Wrote {len(raw)} rows to {args.output_dir}/property_raw.csv")

    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'dbname': os.getenv('POSTGRES_DB', 'property_db'),
        'user': os.getenv('POSTGRES_USER', 'admin'),
        'password': os.getenv('POSTGRES_PASSWORD', 'admin123'),
    }
    print("Seeding src_crm schema...")
    seed_src_crm(owners, agents, conn_params)
    print("Done.")
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/test_generate_data.py -v
```
Expected: 7 tests PASSED

- [ ] **Step 7: Run the generator locally to produce `data/property_raw.csv`**

```bash
mkdir -p data
python scripts/generate_data.py --output-dir data
```
Expected output:
```
Generating properties...
Generating owners...
Generating agents...
Generating transactions...
Wrote 10000 rows to data/property_raw.csv
```

- [ ] **Step 8: Commit**

```bash
git add scripts/ tests/test_generate_data.py
git commit -m "feat: faker data generator for properties, transactions, owners, agents"
```

---

## Phase 3 — Spark Ingestion

### Task 3: PySpark ingestion job

**Files:**
- Create: `spark/Dockerfile`
- Create: `spark/ingest.py`

- [ ] **Step 1: Create `spark/Dockerfile`**

```dockerfile
FROM bitnami/spark:3.5

USER root
RUN pip install psycopg2-binary==2.9.9

USER 1001
```

- [ ] **Step 2: Create `spark/ingest.py`**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import os

JDBC_URL = (
    f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
    f"/{os.environ['POSTGRES_DB']}"
)
JDBC_PROPS = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "driver": "org.postgresql.Driver",
}


def get_spark():
    return (SparkSession.builder
            .appName("property-ingest")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .getOrCreate())


def load_csv_to_bronze(spark: SparkSession, csv_path: str) -> None:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path)
          .withColumn("_loaded_at", F.current_timestamp()))
    (df.write
     .mode("overwrite")
     .jdbc(JDBC_URL, "bronze.property_raw", properties=JDBC_PROPS))
    print(f"Loaded {df.count()} rows into bronze.property_raw")


def load_src_crm_to_bronze(spark: SparkSession, table: str) -> None:
    df = (spark.read
          .jdbc(JDBC_URL, f"src_crm.{table}", properties=JDBC_PROPS)
          .withColumn("_loaded_at", F.current_timestamp()))
    (df.write
     .mode("overwrite")
     .jdbc(JDBC_URL, f"bronze.{table}", properties=JDBC_PROPS))
    print(f"Loaded {df.count()} rows into bronze.{table}")


if __name__ == "__main__":
    spark = get_spark()
    load_csv_to_bronze(spark, "/data/property_raw.csv")
    load_src_crm_to_bronze(spark, "src_crm_owners")
    load_src_crm_to_bronze(spark, "src_crm_agents")
    spark.stop()
    print("Ingestion complete.")
```

- [ ] **Step 3: Build the Spark image**

```bash
docker compose build spark-ingest
```
Expected: `Successfully built` (no errors)

- [ ] **Step 4: Start Postgres and run the generator + ingest**

```bash
docker compose up -d postgres
sleep 5
python scripts/generate_data.py --output-dir data
docker compose run --rm --profile ingest spark-ingest
```
Expected: `Ingestion complete.`

- [ ] **Step 5: Verify bronze tables exist and have data**

```bash
docker compose exec postgres psql -U admin -d property_db -c "\dt bronze.*"
docker compose exec postgres psql -U admin -d property_db -c "SELECT COUNT(*) FROM bronze.property_raw;"
docker compose exec postgres psql -U admin -d property_db -c "SELECT COUNT(*) FROM bronze.src_crm_owners;"
docker compose exec postgres psql -U admin -d property_db -c "SELECT COUNT(*) FROM bronze.src_crm_agents;"
```
Expected: ~10000 rows in `property_raw`, ~500 in `src_crm_owners`, ~100 in `src_crm_agents`

- [ ] **Step 6: Commit**

```bash
git add spark/
git commit -m "feat: pyspark ingestion job loads csv + src_crm into bronze schema"
```

---

## Phase 4 — dbt Setup

### Task 4: dbt project initialisation

**Files:**
- Create: `dbt/Dockerfile`
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`

- [ ] **Step 1: Create `dbt/Dockerfile`**

```dockerfile
FROM python:3.11-slim

RUN pip install dbt-postgres==1.8.3

WORKDIR /dbt
```

- [ ] **Step 2: Create `dbt/dbt_project.yml`**

```yaml
name: 'property_platform'
version: '1.0.0'
config-version: 2

profile: 'property_platform'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]

models:
  property_platform:
    staging:
      +schema: silver
      +materialized: view
    vault:
      hubs:
        +schema: gold
        +materialized: incremental
        +unique_key: ['load_date']
      links:
        +schema: gold
        +materialized: incremental
      satellites:
        +schema: gold
        +materialized: incremental
    mart:
      +schema: mart
      +materialized: table
```

- [ ] **Step 3: Create `dbt/profiles.yml`**

```yaml
property_platform:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') | int }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      schema: silver
      threads: 4
```

- [ ] **Step 4: Build dbt image and verify connection**

```bash
docker compose build dbt
docker compose run --rm --profile dbt dbt dbt debug
```
Expected: `All checks passed!`

- [ ] **Step 5: Commit**

```bash
git add dbt/
git commit -m "feat: dbt project init with postgres adapter"
```

---

## Phase 5 — dbt Staging Models (Silver Layer)

### Task 5: Sources declaration and staging models

**Files:**
- Create: `dbt/models/sources.yml`
- Create: `dbt/models/staging/stg_transactions.sql`
- Create: `dbt/models/staging/stg_properties.sql`
- Create: `dbt/models/staging/stg_owners.sql`
- Create: `dbt/models/staging/stg_agents.sql`
- Create: `dbt/models/staging/staging.yml`

- [ ] **Step 1: Create `dbt/models/sources.yml`**

```yaml
version: 2

sources:
  - name: bronze
    schema: bronze
    tables:
      - name: property_raw
      - name: src_crm_owners
      - name: src_crm_agents
```

- [ ] **Step 2: Create `dbt/models/staging/stg_transactions.sql`**

Note: splits the combined raw CSV into the transactions entity only; computes hash key and hashdiff.

```sql
with source as (
    select * from {{ source('bronze', 'property_raw') }}
),

staged as (
    select
        transaction_id,
        property_id,
        buyer_id,
        seller_id,
        agent_id,
        cast(sale_price as numeric(14,2))     as sale_price,
        cast(sale_date as date)               as sale_date,
        cast(settlement_date as date)         as settlement_date,
        cast(days_on_market as integer)       as days_on_market,
        coalesce(sale_type, 'unknown')        as sale_type,
        'bronze.property_raw'                 as record_source,
        cast(_loaded_at as timestamp)         as load_date,

        md5(cast(transaction_id as text))     as transaction_hk,
        md5(cast(property_id as text))        as property_hk,
        md5(cast(buyer_id as text))           as buyer_hk,
        md5(cast(seller_id as text))          as seller_hk,
        md5(cast(agent_id as text))           as agent_hk,

        md5(concat_ws('||',
            cast(sale_price as text),
            cast(sale_date as text),
            cast(settlement_date as text),
            cast(days_on_market as text),
            sale_type
        ))                                    as hashdiff
    from source
    where transaction_id is not null
)

select * from staged
```

- [ ] **Step 3: Create `dbt/models/staging/stg_properties.sql`**

```sql
with source as (
    select * from {{ source('bronze', 'property_raw') }}
),

deduped as (
    select distinct
        property_id,
        address,
        suburb,
        postcode,
        state,
        cast(bedrooms as integer)        as bedrooms,
        cast(bathrooms as integer)       as bathrooms,
        cast(land_size_sqm as numeric)   as land_size_sqm,
        property_type,
        cast(year_built as integer)      as year_built,
        cast(listing_price as numeric(14,2)) as listing_price,
        cast(_loaded_at as timestamp)    as load_date,
        'bronze.property_raw'            as record_source
    from source
    where property_id is not null
),

staged as (
    select
        *,
        md5(cast(property_id as text))   as property_hk,
        md5(concat_ws('||',
            coalesce(address,''),
            coalesce(suburb,''),
            coalesce(postcode,''),
            coalesce(state,''),
            cast(bedrooms as text),
            cast(bathrooms as text),
            cast(land_size_sqm as text),
            coalesce(property_type,''),
            cast(year_built as text),
            cast(listing_price as text)
        ))                               as hashdiff
    from deduped
)

select * from staged
```

- [ ] **Step 4: Create `dbt/models/staging/stg_owners.sql`**

```sql
with source as (
    select * from {{ source('bronze', 'src_crm_owners') }}
),

staged as (
    select
        owner_id,
        full_name,
        email,
        phone,
        suburb,
        state,
        cast(registration_date as date)  as registration_date,
        cast(investor_flag as boolean)   as investor_flag,
        cast(_loaded_at as timestamp)    as load_date,
        'src_crm.owners'                 as record_source,

        md5(cast(owner_id as text))      as owner_hk,
        md5(concat_ws('||',
            coalesce(full_name,''),
            coalesce(email,''),
            coalesce(phone,''),
            coalesce(suburb,''),
            coalesce(state,''),
            cast(registration_date as text),
            cast(investor_flag as text)
        ))                               as hashdiff
    from source
    where owner_id is not null
)

select * from staged
```

- [ ] **Step 5: Create `dbt/models/staging/stg_agents.sql`**

```sql
with source as (
    select * from {{ source('bronze', 'src_crm_agents') }}
),

staged as (
    select
        agent_id,
        full_name,
        email,
        agency_name,
        license_no,
        region,
        accreditation_tier,
        cast(active_from as date)        as active_from,
        cast(_loaded_at as timestamp)    as load_date,
        'src_crm.agents'                 as record_source,

        md5(cast(agent_id as text))      as agent_hk,
        md5(concat_ws('||',
            coalesce(full_name,''),
            coalesce(email,''),
            coalesce(agency_name,''),
            coalesce(license_no,''),
            coalesce(region,''),
            coalesce(accreditation_tier,''),
            cast(active_from as text)
        ))                               as hashdiff
    from source
    where agent_id is not null
)

select * from staged
```

- [ ] **Step 6: Create `dbt/models/staging/staging.yml` with tests**

```yaml
version: 2

models:
  - name: stg_transactions
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: transaction_hk
        tests: [not_null, unique]
      - name: property_id
        tests: [not_null]
      - name: sale_price
        tests: [not_null]
      - name: sale_date
        tests: [not_null]

  - name: stg_properties
    columns:
      - name: property_id
        tests: [not_null, unique]
      - name: property_hk
        tests: [not_null, unique]
      - name: suburb
        tests: [not_null]

  - name: stg_owners
    columns:
      - name: owner_id
        tests: [not_null, unique]
      - name: owner_hk
        tests: [not_null, unique]

  - name: stg_agents
    columns:
      - name: agent_id
        tests: [not_null, unique]
      - name: agent_hk
        tests: [not_null, unique]
```

- [ ] **Step 7: Run staging models**

```bash
docker compose run --rm --profile dbt dbt dbt run --select staging
```
Expected: `4 of 4 OK`

- [ ] **Step 8: Run staging tests**

```bash
docker compose run --rm --profile dbt dbt dbt test --select staging
```
Expected: all tests pass

- [ ] **Step 9: Commit**

```bash
git add dbt/models/
git commit -m "feat: dbt staging models split bronze into stg_transactions, stg_properties, stg_owners, stg_agents"
```

---

## Phase 6 — dbt Vault Hubs (Gold Layer)

### Task 6: Hub models

**Files:**
- Create: `dbt/models/vault/hubs/hub_property.sql`
- Create: `dbt/models/vault/hubs/hub_owner.sql`
- Create: `dbt/models/vault/hubs/hub_agent.sql`
- Create: `dbt/models/vault/hubs/hub_transaction.sql`

Hubs are insert-only incremental models. They store only the business key, its hash key, first load date, and record source. No attributes.

- [ ] **Step 1: Create `dbt/models/vault/hubs/hub_property.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='property_hk',
    schema='gold'
  )
}}

with source as (
    select
        property_hk,
        property_id,
        load_date,
        record_source
    from {{ ref('stg_properties') }}
)

select * from source

{% if is_incremental() %}
where property_hk not in (select property_hk from {{ this }})
{% endif %}
```

- [ ] **Step 2: Create `dbt/models/vault/hubs/hub_owner.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='owner_hk',
    schema='gold'
  )
}}

with source as (
    select
        owner_hk,
        owner_id,
        load_date,
        record_source
    from {{ ref('stg_owners') }}
)

select * from source

{% if is_incremental() %}
where owner_hk not in (select owner_hk from {{ this }})
{% endif %}
```

- [ ] **Step 3: Create `dbt/models/vault/hubs/hub_agent.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='agent_hk',
    schema='gold'
  )
}}

with source as (
    select
        agent_hk,
        agent_id,
        load_date,
        record_source
    from {{ ref('stg_agents') }}
)

select * from source

{% if is_incremental() %}
where agent_hk not in (select agent_hk from {{ this }})
{% endif %}
```

- [ ] **Step 4: Create `dbt/models/vault/hubs/hub_transaction.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        transaction_id,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 5: Run hub models**

```bash
docker compose run --rm --profile dbt dbt dbt run --select vault.hubs
```
Expected: `4 of 4 OK`

- [ ] **Step 6: Verify hub row counts**

```bash
docker compose exec postgres psql -U admin -d property_db -c "
  SELECT 'hub_property' as hub, count(*) FROM gold.hub_property
  UNION ALL SELECT 'hub_owner', count(*) FROM gold.hub_owner
  UNION ALL SELECT 'hub_agent', count(*) FROM gold.hub_agent
  UNION ALL SELECT 'hub_transaction', count(*) FROM gold.hub_transaction;
"
```
Expected: ~2000, ~500, ~100, ~10000 rows respectively

- [ ] **Step 7: Commit**

```bash
git add dbt/models/vault/hubs/
git commit -m "feat: dbt vault hub models for property, owner, agent, transaction"
```

---

## Phase 7 — dbt Vault Links (Gold Layer)

### Task 7: Link models

**Files:**
- Create: `dbt/models/vault/links/lnk_transaction_property.sql`
- Create: `dbt/models/vault/links/lnk_transaction_buyer.sql`
- Create: `dbt/models/vault/links/lnk_transaction_seller.sql`
- Create: `dbt/models/vault/links/lnk_transaction_agent.sql`

Links record relationships between hubs. Insert-only incremental.

- [ ] **Step 1: Create `dbt/models/vault/links/lnk_transaction_property.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        property_hk,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 2: Create `dbt/models/vault/links/lnk_transaction_buyer.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        buyer_hk      as owner_hk,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 3: Create `dbt/models/vault/links/lnk_transaction_seller.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        seller_hk     as owner_hk,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 4: Create `dbt/models/vault/links/lnk_transaction_agent.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        agent_hk,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 5: Run link models**

```bash
docker compose run --rm --profile dbt dbt dbt run --select vault.links
```
Expected: `4 of 4 OK`

- [ ] **Step 6: Commit**

```bash
git add dbt/models/vault/links/
git commit -m "feat: dbt vault link models for transaction-property, buyer, seller, agent"
```

---

## Phase 8 — dbt Vault Satellites with SCD Type 2 (Gold Layer)

### Task 8: Satellite models

**Files:**
- Create: `dbt/models/vault/satellites/sat_property_details.sql`
- Create: `dbt/models/vault/satellites/sat_owner_details.sql`
- Create: `dbt/models/vault/satellites/sat_agent_details.sql`
- Create: `dbt/models/vault/satellites/sat_transaction_details.sql`
- Create: `dbt/models/vault/vault.yml`

Satellites use incremental materialization. The SCD2 pattern:
1. On each run, only insert rows where `hashdiff` has changed (or new key)
2. A `post_hook` UPDATE closes the previous open row by setting its `load_end_date`
3. Current record = `load_end_date IS NULL`

- [ ] **Step 1: Create `dbt/models/vault/satellites/sat_property_details.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key=['property_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.property_hk = new_row.property_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        property_hk,
        address, suburb, postcode, state,
        bedrooms, bathrooms, land_size_sqm,
        property_type, year_built, listing_price,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_properties') }}
),

{% if is_incremental() %}
latest as (
    select property_hk, hashdiff
    from {{ this }}
    where load_end_date is null
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.property_hk = l.property_hk
    where l.property_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    property_hk,
    address, suburb, postcode, state,
    bedrooms, bathrooms, land_size_sqm,
    property_type, year_built, listing_price,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
```

- [ ] **Step 2: Create `dbt/models/vault/satellites/sat_owner_details.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key=['owner_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.owner_hk = new_row.owner_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        owner_hk,
        full_name, email, phone, suburb, state,
        registration_date, investor_flag,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_owners') }}
),

{% if is_incremental() %}
latest as (
    select owner_hk, hashdiff
    from {{ this }}
    where load_end_date is null
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.owner_hk = l.owner_hk
    where l.owner_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    owner_hk,
    full_name, email, phone, suburb, state,
    registration_date, investor_flag,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
```

- [ ] **Step 3: Create `dbt/models/vault/satellites/sat_agent_details.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key=['agent_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.agent_hk = new_row.agent_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        agent_hk,
        full_name, email, agency_name, license_no,
        region, accreditation_tier, active_from,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_agents') }}
),

{% if is_incremental() %}
latest as (
    select agent_hk, hashdiff
    from {{ this }}
    where load_end_date is null
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.agent_hk = l.agent_hk
    where l.agent_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    agent_hk,
    full_name, email, agency_name, license_no,
    region, accreditation_tier, active_from,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
```

- [ ] **Step 4: Create `dbt/models/vault/satellites/sat_transaction_details.sql`**

Transaction data doesn't change once recorded — append-only satellite (no SCD2 needed).

```sql
{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        sale_price, sale_date, settlement_date,
        days_on_market, sale_type,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select
    transaction_hk,
    sale_price, sale_date, settlement_date,
    days_on_market, sale_type,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
```

- [ ] **Step 5: Create `dbt/models/vault/vault.yml` with relationship tests**

```yaml
version: 2

models:
  - name: hub_property
    columns:
      - name: property_hk
        tests: [not_null, unique]
      - name: property_id
        tests: [not_null, unique]

  - name: hub_owner
    columns:
      - name: owner_hk
        tests: [not_null, unique]

  - name: hub_agent
    columns:
      - name: agent_hk
        tests: [not_null, unique]

  - name: hub_transaction
    columns:
      - name: transaction_hk
        tests: [not_null, unique]

  - name: lnk_transaction_property
    columns:
      - name: transaction_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_transaction')
              field: transaction_hk
      - name: property_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_property')
              field: property_hk

  - name: sat_property_details
    columns:
      - name: property_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_property')
              field: property_hk
      - name: hashdiff
        tests: [not_null]

  - name: sat_owner_details
    columns:
      - name: owner_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_owner')
              field: owner_hk

  - name: sat_agent_details
    columns:
      - name: agent_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_agent')
              field: agent_hk

  - name: sat_transaction_details
    columns:
      - name: transaction_hk
        tests:
          - not_null
          - relationships:
              to: ref('hub_transaction')
              field: transaction_hk
```

- [ ] **Step 6: Run satellite models**

```bash
docker compose run --rm --profile dbt dbt dbt run --select vault.satellites
```
Expected: `4 of 4 OK`

- [ ] **Step 7: Run vault tests**

```bash
docker compose run --rm --profile dbt dbt dbt test --select vault
```
Expected: all tests pass

- [ ] **Step 8: Verify SCD2 is working by simulating a data change**

```bash
# Update one property's suburb in the bronze table
docker compose exec postgres psql -U admin -d property_db -c "
  UPDATE bronze.property_raw
  SET suburb = 'SydneyUpdated', _loaded_at = now()
  WHERE property_id = (SELECT property_id FROM bronze.property_raw LIMIT 1);
"

# Re-run staging + satellites
docker compose run --rm --profile dbt dbt dbt run --select stg_properties sat_property_details

# Verify two rows exist for that property_hk, one with load_end_date set
docker compose exec postgres psql -U admin -d property_db -c "
  SELECT property_hk, suburb, load_date, load_end_date
  FROM gold.sat_property_details
  WHERE property_hk = (
    SELECT md5(property_id) FROM bronze.property_raw
    WHERE suburb = 'SydneyUpdated' LIMIT 1
  )
  ORDER BY load_date;
"
```
Expected: 2 rows — first has `load_end_date` set, second has `load_end_date IS NULL` and `suburb = 'SydneyUpdated'`

- [ ] **Step 9: Commit**

```bash
git add dbt/models/vault/
git commit -m "feat: dbt vault satellites with SCD2 incremental + post_hook for load_end_date"
```

---

## Phase 9 — dbt Mart Models

### Task 9: Mart models

**Files:**
- Create: `dbt/models/mart/mart_property_sales.sql`
- Create: `dbt/models/mart/mart_agent_performance.sql`
- Create: `dbt/models/mart/mart_suburb_trends.sql`
- Create: `dbt/models/mart/mart_owner_portfolio.sql`
- Create: `dbt/models/mart/mart.yml`

All marts use `load_end_date IS NULL` to get the current version of satellite records.

- [ ] **Step 1: Create `dbt/models/mart/mart_property_sales.sql`**

```sql
{{
  config(schema='mart', materialized='table')
}}

with transactions as (
    select * from {{ ref('hub_transaction') }} ht
    join {{ ref('sat_transaction_details') }} std
        on ht.transaction_hk = std.transaction_hk
        and std.load_end_date is null
),

properties as (
    select hp.property_hk, hp.property_id, sp.*
    from {{ ref('hub_property') }} hp
    join {{ ref('sat_property_details') }} sp
        on hp.property_hk = sp.property_hk
        and sp.load_end_date is null
),

agents as (
    select ha.agent_hk, ha.agent_id, sa.full_name as agent_name, sa.agency_name, sa.region
    from {{ ref('hub_agent') }} ha
    join {{ ref('sat_agent_details') }} sa
        on ha.agent_hk = sa.agent_hk
        and sa.load_end_date is null
),

lnk_prop as (select * from {{ ref('lnk_transaction_property') }}),
lnk_agent as (select * from {{ ref('lnk_transaction_agent') }})

select
    t.transaction_id,
    t.sale_date,
    t.sale_price,
    t.sale_type,
    t.days_on_market,
    t.settlement_date,
    p.property_id,
    p.address,
    p.suburb,
    p.state,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.land_size_sqm,
    p.listing_price,
    round(100.0 * (t.sale_price - p.listing_price) / p.listing_price, 2) as price_vs_listing_pct,
    a.agent_name,
    a.agency_name,
    a.region as agent_region
from transactions t
join lnk_prop lp on t.transaction_hk = lp.transaction_hk
join properties p on lp.property_hk = p.property_hk
left join lnk_agent la on t.transaction_hk = la.transaction_hk
left join agents a on la.agent_hk = a.agent_hk
```

- [ ] **Step 2: Create `dbt/models/mart/mart_agent_performance.sql`**

```sql
{{
  config(schema='mart', materialized='table')
}}

with sales as (select * from {{ ref('mart_property_sales') }})

select
    agent_name,
    agency_name,
    agent_region,
    count(*)                                      as total_transactions,
    round(avg(sale_price), 0)                    as avg_sale_price,
    round(avg(days_on_market), 1)                as avg_days_on_market,
    round(avg(price_vs_listing_pct), 2)          as avg_price_vs_listing_pct,
    min(sale_date)                               as first_sale_date,
    max(sale_date)                               as last_sale_date
from sales
where agent_name is not null
group by agent_name, agency_name, agent_region
```

- [ ] **Step 3: Create `dbt/models/mart/mart_suburb_trends.sql`**

```sql
{{
  config(schema='mart', materialized='table')
}}

with sales as (select * from {{ ref('mart_property_sales') }})

select
    suburb,
    state,
    date_trunc('month', sale_date)               as sale_month,
    count(*)                                     as transaction_count,
    round(avg(sale_price), 0)                   as avg_sale_price,
    round(percentile_cont(0.5) within group
          (order by sale_price), 0)             as median_sale_price,
    round(avg(days_on_market), 1)               as avg_days_on_market
from sales
group by suburb, state, date_trunc('month', sale_date)
```

- [ ] **Step 4: Create `dbt/models/mart/mart_owner_portfolio.sql`**

```sql
{{
  config(schema='mart', materialized='table')
}}

with owners as (
    select ho.owner_id, so.full_name, so.suburb, so.state, so.investor_flag
    from {{ ref('hub_owner') }} ho
    join {{ ref('sat_owner_details') }} so
        on ho.owner_hk = so.owner_hk
        and so.load_end_date is null
),

buyers as (
    select lb.owner_hk, count(*) as buy_count
    from {{ ref('lnk_transaction_buyer') }} lb
    group by lb.owner_hk
),

sellers as (
    select ls.owner_hk, count(*) as sell_count
    from {{ ref('lnk_transaction_seller') }} ls
    group by ls.owner_hk
),

hub_owners as (select * from {{ ref('hub_owner') }})

select
    o.owner_id,
    o.full_name,
    o.suburb,
    o.state,
    o.investor_flag,
    coalesce(b.buy_count, 0)      as total_purchases,
    coalesce(s.sell_count, 0)     as total_sales,
    coalesce(b.buy_count, 0)
      - coalesce(s.sell_count, 0) as net_properties_held
from owners o
join hub_owners ho on o.owner_id = ho.owner_id
left join buyers b on ho.owner_hk = b.owner_hk
left join sellers s on ho.owner_hk = s.owner_hk
```

- [ ] **Step 5: Create `dbt/models/mart/mart.yml`**

```yaml
version: 2

models:
  - name: mart_property_sales
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: sale_price
        tests: [not_null]
      - name: suburb
        tests: [not_null]

  - name: mart_agent_performance
    columns:
      - name: agent_name
        tests: [not_null, unique]
      - name: total_transactions
        tests: [not_null]

  - name: mart_suburb_trends
    columns:
      - name: suburb
        tests: [not_null]
      - name: sale_month
        tests: [not_null]

  - name: mart_owner_portfolio
    columns:
      - name: owner_id
        tests: [not_null, unique]
```

- [ ] **Step 6: Run all mart models**

```bash
docker compose run --rm --profile dbt dbt dbt run --select mart
```
Expected: `4 of 4 OK`

- [ ] **Step 7: Run all dbt tests**

```bash
docker compose run --rm --profile dbt dbt dbt test
```
Expected: all tests pass

- [ ] **Step 8: Commit**

```bash
git add dbt/models/mart/
git commit -m "feat: dbt mart models for property_sales, agent_performance, suburb_trends, owner_portfolio"
```

---

## Phase 10 — Airflow Orchestration

### Task 10: Airflow DAG

**Files:**
- Create: `airflow/Dockerfile`
- Create: `airflow/dags/property_pipeline.py`

- [ ] **Step 1: Update `spark/Dockerfile` to include the generator script**

The Airflow DAG runs both the data generator and the Spark ingest job inside the same `spark-ingest` container, so `generate_data.py` must be bundled into it.

```dockerfile
FROM bitnami/spark:3.5

USER root
RUN pip install psycopg2-binary==2.9.9 faker==24.14.1 pandas==2.2.2

COPY ../scripts/generate_data.py /app/generate_data.py
COPY ingest.py /app/ingest.py

USER 1001
```

Also update `docker-compose.yml` spark-ingest volumes to remove the script bind mount (it's now baked in):

```yaml
  spark-ingest:
    build: ./spark
    environment:
      <<: *postgres-env
    volumes:
      - ./data:/data
    depends_on:
      postgres:
        condition: service_healthy
    command: ["spark-submit", "--packages", "org.postgresql:postgresql:42.7.1", "/app/ingest.py"]
    profiles: ["ingest"]
```

- [ ] **Step 2: Create `airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install apache-airflow-providers-docker==3.9.0
```

- [ ] **Step 2: Create `airflow/dags/property_pipeline.py`**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='property_pipeline',
    default_args=default_args,
    description='Ingest property data, run dbt transforms and tests',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['property', 'dbt', 'spark'],
) as dag:

    generate_and_ingest = DockerOperator(
        task_id='generate_and_ingest',
        image='dbt-practice-spark-ingest:latest',
        command='bash -c "python /app/generate_data.py --output-dir /data && spark-submit --packages org.postgresql:postgresql:42.7.1 /app/ingest.py"',
        mounts=[
            Mount(source='/data', target='/data', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='dbt-practice-dbt:latest',
        command='dbt run',
        working_dir='/dbt',
        mounts=[
            Mount(source='{{ var.value.get("dbt_project_path", "/opt/dbt") }}',
                  target='/dbt', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='dbt-practice-dbt:latest',
        command='dbt test',
        working_dir='/dbt',
        mounts=[
            Mount(source='{{ var.value.get("dbt_project_path", "/opt/dbt") }}',
                  target='/dbt', type='bind'),
        ],
        environment={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'property_db',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin123',
        },
        network_mode='dbt-practice_default',
        auto_remove='success',
        docker_url='unix:///var/run/docker.sock',
    )

    generate_and_ingest >> dbt_run >> dbt_test
```

- [ ] **Step 3: Build Airflow image**

```bash
docker compose build airflow
```
Expected: `Successfully built`

- [ ] **Step 4: Start Airflow**

```bash
docker compose up -d airflow
```

- [ ] **Step 5: Verify Airflow UI is running**

Open http://localhost:8080 in browser. Login with `admin` / `admin`.
Expected: Airflow UI loads, `property_pipeline` DAG is visible.

- [ ] **Step 6: Trigger DAG manually and verify all 3 tasks succeed**

In Airflow UI: click `property_pipeline` → click the play ▶ button → trigger DAG.
Expected: `generate_and_ingest` → `dbt_run` → `dbt_test` all show green (success).

- [ ] **Step 7: Commit**

```bash
git add airflow/
git commit -m "feat: airflow dag property_pipeline orchestrates ingest -> dbt run -> dbt test"
```

---

## Phase 11 — Superset Dashboard

### Task 11: Superset setup and dashboards

**Files:**
- Create: `superset/init.sh`

- [ ] **Step 1: Create `superset/init.sh`**

```bash
#!/bin/bash
# Registers the property_db PostgreSQL connection in Superset via its CLI
superset set-database-uri \
  -d "Property DB" \
  -u "postgresql+psycopg2://admin:admin123@postgres:5432/property_db"
```

- [ ] **Step 2: Start Superset**

```bash
docker compose up -d superset
```
Wait ~30 seconds for Superset to initialise, then open http://localhost:8088.
Login: `admin` / `admin`

- [ ] **Step 3: Add database connection manually in UI**

In Superset: Settings → Database Connections → + Database → PostgreSQL

```
Host:     postgres
Port:     5432
Database: property_db
Username: admin
Password: admin123
```
Click "Test Connection" → Expected: "Connection looks good!"
Save.

- [ ] **Step 4: Create datasets for each mart table**

In Superset: Datasets → + Dataset

Add each table from the `mart` schema:
1. `mart.mart_property_sales`
2. `mart.mart_agent_performance`
3. `mart.mart_suburb_trends`
4. `mart.mart_owner_portfolio`

- [ ] **Step 5: Create "Property Sales Overview" dashboard**

In Superset: Charts → + Chart → select `mart_property_sales`

Create these charts and add to a new dashboard called **"Property Sales Overview"**:

| Chart | Type | X-axis | Metric |
|---|---|---|---|
| Sales Volume Over Time | Bar Chart | `sale_date` (month) | `COUNT(*)` |
| Median Price by Suburb | Bar Chart | `suburb` | `MEDIAN(sale_price)` |
| Sale Type Breakdown | Pie Chart | `sale_type` | `COUNT(*)` |

- [ ] **Step 6: Create remaining dashboards**

**Agent Performance** dashboard using `mart_agent_performance`:
- Top Agents by Volume: Bar Chart — `agent_name` vs `total_transactions`
- Avg Days on Market by Agent: Bar Chart — `agent_name` vs `avg_days_on_market`

**Suburb Trends** dashboard using `mart_suburb_trends`:
- Median Price Trend: Line Chart — `sale_month` x-axis, `median_sale_price`, grouped by `suburb`

**Owner Portfolio** dashboard using `mart_owner_portfolio`:
- Investor vs Owner: Pie Chart — `investor_flag` vs `COUNT(*)`
- Net Properties Held Distribution: Histogram — `net_properties_held`

- [ ] **Step 7: Commit**

```bash
git add superset/
git commit -m "feat: superset init script and dashboard setup instructions"
```

---

## Phase 12 — End-to-End Verification

### Task 12: Full stack smoke test

- [ ] **Step 1: Start all services**

```bash
docker compose up -d postgres airflow superset
```

- [ ] **Step 2: Trigger pipeline via Airflow**

Open http://localhost:8080 → trigger `property_pipeline` DAG.
Wait for all 3 tasks to complete (green).

- [ ] **Step 3: Verify all schemas have data**

```bash
docker compose exec postgres psql -U admin -d property_db -c "
  SELECT schemaname, tablename, n_live_tup AS row_count
  FROM pg_stat_user_tables
  WHERE schemaname IN ('bronze','silver','gold','mart')
  ORDER BY schemaname, tablename;
"
```
Expected: all tables have non-zero `row_count`

- [ ] **Step 4: Verify SCD2 works end-to-end**

```bash
# Change an agent's accreditation_tier in bronze
docker compose exec postgres psql -U admin -d property_db -c "
  UPDATE bronze.src_crm_agents
  SET accreditation_tier = 'platinum', _loaded_at = now()
  WHERE agent_id = (SELECT agent_id FROM bronze.src_crm_agents LIMIT 1);
"

# Trigger pipeline again
# (In Airflow UI, trigger property_pipeline again)

# Verify two sat_agent_details rows exist for that agent
docker compose exec postgres psql -U admin -d property_db -c "
  SELECT agent_hk, accreditation_tier, load_date, load_end_date
  FROM gold.sat_agent_details
  WHERE agent_hk = (
    SELECT md5(agent_id) FROM bronze.src_crm_agents
    WHERE accreditation_tier = 'platinum' LIMIT 1
  )
  ORDER BY load_date;
"
```
Expected: 2 rows — original with `load_end_date` set, new row with `accreditation_tier = 'platinum'` and `load_end_date IS NULL`

- [ ] **Step 5: Verify Superset dashboards load**

Open http://localhost:8088 → verify all 4 dashboards render with data.

- [ ] **Step 6: Final commit**

```bash
git add .
git commit -m "chore: end-to-end verification complete"
```

---

## Summary

| Phase | Tasks | Output |
|---|---|---|
| Infrastructure | Task 1 | Docker Compose with 5 services |
| Data Generation | Task 2 | Faker generator + tests |
| Spark Ingestion | Task 3 | PySpark loads bronze schema |
| dbt Setup | Task 4 | dbt project wired to Postgres |
| Staging | Task 5 | Silver layer: 4 staging views with hash keys |
| Hubs | Task 6 | 4 DV2 hub tables |
| Links | Task 7 | 4 DV2 link tables |
| Satellites | Task 8 | 4 DV2 satellites with SCD2 |
| Mart | Task 9 | 4 business-ready mart tables |
| Airflow | Task 10 | 3-task daily pipeline DAG |
| Superset | Task 11 | 4 dashboards on mart schema |
| Verification | Task 12 | End-to-end smoke test |
