"""
ETL Challenge
This Python script exports data from production database "arcadia" to target
test database "testarcadia" on a local Postgres instance using SQLAlchemy.

Alvin Kim
17th Feb 2020
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, Text, Date

# Input credentials for each database. Probably should use SSH.
userProd = 'postgres'
passwordProd = '***PASSWORD GOES HERE***'
userTest = 'postgres'
passwordTest = '***PASSWORD GOES HERE***'

# Create engines for each database
prod_engine = create_engine(
    'postgresql://{}:{}@localhost:5432/arcadia'.format(userProd, passwordProd),
    echo=True)
prod_session = sessionmaker(bind=prod_engine)
session1 = prod_session()

test_engine = create_engine(
    'postgresql://{}:{}@localhost:5432/testarcadia'.format(userTest, passwordTest),
    echo=True)
test_session = sessionmaker(bind=test_engine)
session2 = test_session()

# Ingests tables from "arcadia" database into pandas dataframe.
# PII is MD5 hashed using the name and encrypted username. SHA256 is preferred,
# however that would require installation of Postgres plugin "pgcrypto" or python hashlib.
# Specific addresses are omitted.
material_accounts = pd.read_sql('''SELECT acc.id AS account_id,
       MD5(ROW(name,encrypted_username)::TEXT) AS hash_id,
       account_number,
       status,
       ad.line2 AS market
       FROM arcadia.public.account acc
       JOIN arcadia.public.address ad ON ad.account_id = acc.id;''', con=prod_engine)

statements = pd.read_sql('''SELECT *
    FROM arcadia.public.statement statement;''', con=prod_engine)

session1.close()
print("Prod session closed")

# Write tables from dataframe to "testarcadia" database.
material_accounts.to_sql("material_account",
                         test_engine,
                         if_exists='replace',
                         schema='public',
                         index=False,
                         chunksize=500,
                         dtype={"account_id": Integer,
                                "hash_id": Text,
                                "account_number": Text,
                                "status": Text,
                                "market": Text
                                })

statements.to_sql("statement",
                         test_engine,
                         if_exists='replace',
                         schema='public',
                         index=False,
                         chunksize=500,
                         dtype={"id": Integer,
                                "account_id": Integer,
                                "start_date": Date,
                                "end_date": Date,
                                "useage": Integer,
                                "charges": Text, # Originally "money", but cast to "text".
                                "status": Text
                                })
session2.commit()
session2.close()
print("Test session closed")
print('Tables successfully migrated.')