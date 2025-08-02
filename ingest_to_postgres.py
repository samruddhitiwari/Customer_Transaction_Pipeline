import psycopg2
from sqlalchemy import create_engine

engine = create_engine("postgresql://username:password@localhost:5432/bank")
customers_df.to_sql("customers", engine, if_exists='replace', index=False)
transactions_df.to_sql("transactions", engine, if_exists='replace', index=False)
