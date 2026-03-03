from src.extract.extract import run_extract
import pandas as pd
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

transform_logger = logging.getLogger("transform")

def transform_df(data:list) -> pd.DataFrame:
    return pd.DataFrame(data)

def drop_null(df:pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    after = len(df.dropna(subset=['value']))
    removed = before - after
    transform_logger.info(f"Removendo {removed} valores nulos")
    return df

def change_year_type(df:pd.DataFrame) -> pd.DataFrame:
    transform_logger.info(f"Transformando o tipo da coluna year")
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    return df

def run_transform():
    transform_logger.info("Iniciando processo de transformacao")
    data = run_extract()
    df = transform_df(data)
    transform_logger.info("Json tranformado em df com sucesso!")
    df = change_year_type(df)
    df = drop_null(df)
    transform_logger.info("Processo de transformacao finalizado com sucesso")
    return df
