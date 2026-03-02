from src.extract.extract import run_extract
import pandas as pd


def transform_df(data:list) -> pd.DataFrame:
    return pd.DataFrame(data)

def drop_null(df:pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=['value'])
    return df

def change_year_type(df:pd.DataFrame) -> pd.DataFrame:
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    return df

def run_transform():
    data = run_extract()
    df = transform_df(data)
    df = change_year_type(df)
    df = drop_null(df)
    return df
