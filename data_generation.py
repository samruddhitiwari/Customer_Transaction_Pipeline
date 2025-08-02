from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()

NUM_CUSTOMERS = 200
NUM_TRANSACTIONS = 1000

# Generate customer data
def generate_customers():
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            'customer_id': i + 1,
            'name': fake.name(),
            'email': fake.email(),
            'created_at': fake.date_between(start_date='-2y', end_date='today')
        })
    return pd.DataFrame(customers)

# Generate transactions
def generate_transactions(customers_df):
    transactions = []
    for i in range(NUM_TRANSACTIONS):
        cust = customers_df.sample().iloc[0]
        transactions.append({
            'transaction_id': i + 1,
            'customer_id': cust.customer_id,
            'amount': round(random.uniform(10.0, 5000.0), 2),
            'category': random.choice(['Groceries', 'Electronics', 'Bills', 'Travel', 'Health']),
            'timestamp': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return pd.DataFrame(transactions)

customers_df = generate_customers()
transactions_df = generate_transactions(customers_df)

customers_df.to_csv("customers.csv", index=False)
transactions_df.to_csv("transactions.csv", index=False)
