# Importação da função de extração e bibliotecas essenciais
from src.extract.extract import run_extract  # Função que traz os dados brutos (JSON, API, etc.)
import pandas as pd                          # Biblioteca padrão para manipulação de dados tabulares
import logging                               # Biblioteca padrão para logs e monitoramento do pipeline

# Configuração global do logging
logging.basicConfig(
    level=logging.INFO,  # Define que mensagens INFO e superiores serão exibidas
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # Formato detalhado para rastreabilidade
)
# Criação de um logger específico para o módulo de transformação
transform_logger = logging.getLogger("transform")

# Função para transformar a lista de dicionários ou JSON em DataFrame do Pandas
def transform_df(data: list) -> pd.DataFrame:
    """
    Converte dados extraídos (lista/dicionário) em DataFrame.
    Type hinting ajuda na clareza do portfólio.
    """
    # Sugestão futura: validar se data não está vazio antes de criar o DataFrame
    return pd.DataFrame(data)

# Função para remover valores nulos da coluna 'value'
def drop_null(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas com valores nulos na coluna 'value' e loga quantos foram removidos.
    """
    before = len(df)
    # Corrigido: efetivamente removendo os valores nulos
    df_clean = df.dropna(subset=['value'])
    after = len(df_clean)
    removed = before - after
    transform_logger.info(f"Removendo {removed} valores nulos")  # Log de qualidade de dados
    return df_clean

# Função para converter a coluna 'year' para tipo numérico seguro
def change_year_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte a coluna 'year' para tipo inteiro, permitindo NaN para valores inválidos.
    """
    transform_logger.info("Transformando o tipo da coluna 'year'")
    # pd.to_numeric com errors='coerce' garante que valores inválidos se tornem NaN
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    return df

# Função principal do módulo de transformação
def run_transform():
    """
    Função central que:
    1. Chama a extração
    2. Transforma os dados em DataFrame
    3. Converte tipos
    4. Remove nulos
    5. Loga cada etapa
    """
    transform_logger.info("Iniciando processo de transformação")
    
    # Extração de dados brutos
    data = run_extract()
    
    # Transformação inicial em DataFrame
    df = transform_df(data)
    transform_logger.info("JSON transformado em DataFrame com sucesso!")
    
    # Conversão de tipos
    df = change_year_type(df)
    
    # Remoção de valores nulos
    df = drop_null(df)
    
    transform_logger.info("Processo de transformação finalizado com sucesso")
    return df
