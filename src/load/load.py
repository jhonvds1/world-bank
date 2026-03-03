from src.transform.transform import run_transform
import psycopg2
import os
from psycopg2.extras import execute_values
from src.transform.transform import run_transform
import pandas as pd


def connect_db():
    conn = psycopg2.connect(
        host = "localhost",
        port = 5434,
        dbname = "world_bank",
        user = "postgres",
        password = "postgres",
    )
    return conn

def create_tables(cursor):
    query = """
    CREATE TABLE IF NOT EXISTS dim_country(
        country_id VARCHAR(100) PRIMARY KEY,
        country_name VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS dim_indicator(
        indicator_id VARCHAR(100) PRIMARY KEY,
        indicator_name VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS fact_indicators(
        country_id VARCHAR(100) REFERENCES dim_country(country_id),
        indicator_id VARCHAR(100) REFERENCES dim_indicator(indicator_id),
        value NUMERIC(20,2),
        year INT,
        PRIMARY KEY (country_id, indicator_id, year)
    );
    """
    cursor.execute(query)
    

def insert_values(cursor, df):
    execute_values(cursor,"""
        INSERT INTO dim_country (country_id, country_name)
        VALUES %s
        ON CONFLICT (country_id) DO NOTHING;
""", df[['country_id', 'country']].drop_duplicates().values.tolist())

    execute_values(cursor, """
        INSERT INTO dim_indicator (indicator_id, indicator_name)
        VALUES %s
        ON CONFLICT (indicator_id) DO NOTHING;
""",df[['indicator_id', 'indicator']].drop_duplicates().values.tolist())

    execute_values(cursor, """
        INSERT INTO fact_indicators (country_id, indicator_id, value, year)
        VALUES %s
        ON CONFLICT (country_id, indicator_id, year) DO NOTHING;
""", df[['country_id', 'indicator_id', 'value', 'year']].drop_duplicates().values.tolist())

def run_load():
    df = run_transform()
    conn = connect_db()
    cursor = conn.cursor()
    create_tables(cursor)
    insert_values(cursor, df)
    conn.commit()
    cursor.close()
    conn.close()

