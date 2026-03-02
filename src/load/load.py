from src.transform.transform import run_transform
import psycopg2
import os


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
                country_id VARCHAR(5) PRIMARY KEY,
                country_name VARCHAR(5)
            );
    
            CREATE TABLE IF NOT EXISTS dim_indicator(
                indicator_id VARCHAR(20) PRIMARY KEY,
                indicator_name VARCHAR(20)
            );

            CREATE TABLE IF NOT EXISTS fact_indicators(
                country_id VARCHAR(5) REFERENCES dim_country(country_id) ,
                indicator_id VARCHAR(20) REFERENCES dim_indicator(indicator_id),
                value NUMERIC(20,2)
            );
    """
    cursor.execute(query)

def insert_values():
    ...

def run_load():
    conn = connect_db()
    cursor = conn.cursor()
    create_tables(cursor)

run_load()