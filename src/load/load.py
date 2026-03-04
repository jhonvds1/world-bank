from src.transform.transform import run_transform  # Função que transforma os dados, preparando-os para serem carregados
import psycopg2                                     # Conector para PostgreSQL
from psycopg2.extras import execute_values         # Permite inserir várias linhas de forma eficiente
import logging                                     # Para acompanhar o que acontece durante a execução
import os                                          # Para ler variáveis de ambiente, deixando o código mais flexível

# Configura o logging para mostrar mensagens de informação no terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
load_logger = logging.getLogger("load")  # Logger específico para acompanhar a etapa de carga (load)

def connect_db():
    """
    Conecta ao banco de dados PostgreSQL usando variáveis de ambiente.
    Se não estiverem definidas, usa valores padrão (útil para desenvolvimento local e Docker).
    Retorna a conexão para ser usada no ETL.
    """
    load_logger.info("Conectando no banco de dados...")
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),          
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),   
        password=os.getenv("DB_PASSWORD"),  
        port=5432                                 
    )
    load_logger.info("Conexão estabelecida com sucesso!")
    return conn

def create_tables(cursor):
    """
    Cria as tabelas necessárias para o projeto caso ainda não existam:
    - dim_country: tabela dimensão de países
    - dim_indicator: tabela dimensão de indicadores econômicos
    - fact_indicators: tabela fato que relaciona país, indicador, valor e ano
    Uso de chaves primárias e referências garante integridade dos dados.
    """
    load_logger.info("Criando tabelas no banco de dados...")
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
    """
    Insere os dados transformados no banco de dados.
    Usa execute_values para carregar várias linhas de uma vez, melhorando performance.
    ON CONFLICT DO NOTHING evita duplicações caso o ETL seja rodado mais de uma vez.
    """
    load_logger.info("Inserindo dados no banco de dados...")

    # Inserção na tabela de países
    execute_values(
        cursor,
        """
        INSERT INTO dim_country (country_id, country_name)
        VALUES %s
        ON CONFLICT (country_id) DO NOTHING;
        """,
        df[['country_id', 'country']].drop_duplicates().values.tolist()
    )
    load_logger.info(f"{cursor.rowcount} linhas adicionadas em dim_country")

    # Inserção na tabela de indicadores
    execute_values(
        cursor,
        """
        INSERT INTO dim_indicator (indicator_id, indicator_name)
        VALUES %s
        ON CONFLICT (indicator_id) DO NOTHING;
        """,
        df[['indicator_id', 'indicator']].drop_duplicates().values.tolist()
    )
    load_logger.info(f"{cursor.rowcount} linhas adicionadas em dim_indicator")

    # Inserção na tabela fato
    execute_values(
        cursor,
        """
        INSERT INTO fact_indicators (country_id, indicator_id, value, year)
        VALUES %s
        ON CONFLICT (country_id, indicator_id, year) DO NOTHING;
        """,
        df[['country_id', 'indicator_id', 'value', 'year']].drop_duplicates().values.tolist()
    )
    load_logger.info(f"{cursor.rowcount} linhas adicionadas em fact_indicators")
    load_logger.info("Carga finalizada com sucesso")

def run_load():
    """
    Função principal que executa todo o processo de carga (load):
    1. Pega os dados transformados da função run_transform
    2. Conecta no banco de dados
    3. Cria as tabelas se necessário
    4. Insere os dados nas tabelas
    5. Comita e fecha a conexão
    Essa função garante que o ETL seja executado de forma organizada e rastreável.
    """
    load_logger.info("Iniciando processo de load...")
    df = run_transform()        # Obtemos os dados já transformados e prontos para carga
    conn = connect_db()         # Conectamos ao banco
    cursor = conn.cursor()      
    create_tables(cursor)       # Garantimos que as tabelas existem
    insert_values(cursor, df)   # Inserimos os dados no banco
    conn.commit()               # Salvamos todas as alterações
    cursor.close()              
    conn.close()                
    load_logger.info("Load concluído com sucesso")