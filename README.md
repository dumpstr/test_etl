# test_etl
ETL mockup. Moves data from a production database to a test database. 
Obfuscates and omits PII from production database before loading to test databse.
This examples assumes two different PostgreSQL databases on the same host (localhost).
Production database is named "arcadia".
Test database is named "testarcadia".

Before you run: Create a new database "testarcadia" and schema "public" in psql.
  CREATE DATABASE testarcadia;
  \c testarcadia;
  CREATE SCHEMA IF NOT EXISTS public;

Instructions
1. Open etl_mockup.py in IDE of choice.
2. Install all packages in requirements.txt.
3. Enter credentials for production and test databases (lines 16-19).
4. Run code.
