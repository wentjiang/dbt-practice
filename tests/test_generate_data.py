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
