from src.transform.transform import run_transform
import psycopg2
import os
from psycopg2.extras import execute_values
import pandas as pd
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

load_logger = logging.getLogger("load")

def connect_db():
    load_logger.info("Conectando-se ao banco de dados")
    conn = psycopg2.connect(
        host = "host.docker.internal",
        port = 5432,
        dbname = "world_bank",
        user = "postgres",
        password = "postgres",
    )
    load_logger.info("Conexao realizada com sucesso!")
    return conn


def create_tables(cursor):
    load_logger.info("Iniciando criacao de tabelas")
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
    load_logger.info("Tabelas criadas com sucesso")

    

def insert_values(cursor, df):
    load_logger.info("Iniciando processo de carga no banco de dados")
    execute_values(cursor,"""
        INSERT INTO dim_country (country_id, country_name)
        VALUES %s
        ON CONFLICT (country_id) DO NOTHING;
""", df[['country_id', 'country']].drop_duplicates().values.tolist())
    load_logger.info(f"{cursor.rowcount} linhas afetadas em dim_country")

    execute_values(cursor, """
        INSERT INTO dim_indicator (indicator_id, indicator_name)
        VALUES %s
        ON CONFLICT (indicator_id) DO NOTHING;
""",df[['indicator_id', 'indicator']].drop_duplicates().values.tolist())
    load_logger.info(f"{cursor.rowcount} linhas afetadas em dim_indicator")


    execute_values(cursor, """
        INSERT INTO fact_indicators (country_id, indicator_id, value, year)
        VALUES %s
        ON CONFLICT (country_id, indicator_id, year) DO NOTHING;
""", df[['country_id', 'indicator_id', 'value', 'year']].drop_duplicates().values.tolist())
    load_logger.info(f"{cursor.rowcount} linhas afetadas em fact_indicators")


    load_logger.info("Processo de carga no banco de dados finalizado")



def run_load():
    load_logger.info("Iniciando o processo de load no banco de dados")
    df = run_transform()
    conn = connect_db()
    cursor = conn.cursor()
    create_tables(cursor)
    insert_values(cursor, df)
    conn.commit()
    cursor.close()
    conn.close()
    load_logger.info("Processo de load no banco de dados Finalizado com sucesso!")


