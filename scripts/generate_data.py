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
            'investor_flag': random.choice([True, False, False]),
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
    try:
        seed_src_crm(owners, agents, conn_params)
        print("Done.")
    except Exception as e:
        print(f"Warning: could not seed src_crm (postgres not running?): {e}")
