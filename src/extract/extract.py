import requests  # Para fazer requisições HTTP à API do World Bank
import logging   # Para acompanhar o que acontece durante a execução

# Configuração do logging para mostrar informações no terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
extract_logger = logging.getLogger("extract")  # Logger específico para a etapa de extração

def extract_worldbank_data(countries:list, indicators:list)->list:
    """
    Função que consulta a API do World Bank e retorna os dados de indicadores econômicos
    para uma lista de países e indicadores fornecidos.
    
    - countries: lista de códigos de países (ex: BRA, USA)
    - indicators: lista de códigos de indicadores (ex: NY.GDP.MKTP.CD)
    Retorna uma lista de dicionários com os dados extraídos.
    """
    data = []  # Lista que vai armazenar todos os registros extraídos
    for country in countries:  # Itera por cada país
        for indicator in indicators:  # Itera por cada indicador
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json"
            try:
                response = requests.get(url)       # Faz a requisição à API
                response.raise_for_status()        # Garante que retornou sucesso (200)
                json_data = response.json()        # Converte resposta para JSON
                
                # A API retorna uma lista com metadados + dados; verificamos se há registros
                if len(json_data) > 1 and json_data is not None:
                    extract_logger.info(f"Extraindo {len(json_data[1])} dados de: {url}")
                    for record in json_data[1]:  # Itera pelos registros reais
                        data.append({
                            "country": record["country"]["value"],      # Nome do país
                            "country_id": record["country"]["id"],      # Código do país
                            "indicator": record["indicator"]["value"],  # Nome do indicador
                            "indicator_id": record["indicator"]["id"],  # Código do indicador
                            "year": record["date"],                     # Ano da medição
                            "value": record["value"]                    # Valor do indicador
                        })
            except requests.exceptions.RequestException as e:
                # Loga qualquer erro de conexão ou requisição e interrompe execução
                extract_logger.warning(f"Erro ao buscar {country} - {indicator}: {e}")
                raise
            extract_logger.info(f"Dados de {url} extraídos com sucesso")  # Log de sucesso por URL
    return data  # Retorna todos os dados coletados

def run_extract()->list:
    """
    Função principal que executa o processo de extração:
    1. Define países e indicadores que queremos extrair
    2. Chama a função extract_worldbank_data
    3. Retorna os dados para serem transformados e carregados no ETL
    """
    extract_logger.info("Iniciando processo de extração de dados")
    countries = ["BRA", "USA", "CHN", "IND", "DEU"]  # Países de interesse
    indicators = [
        "NY.GDP.MKTP.CD",     # PIB total do país em dólares atuais
        "NY.GDP.PCAP.CD",     # PIB per capita → riqueza média por pessoa
        "SP.POP.TOTL",        # População total
        "SP.DYN.LE00.IN",     # Expectativa de vida ao nascer
        "SL.UEM.TOTL.ZS",     # Taxa de desemprego (%)
        "NY.GDP.MKTP.KD.ZG"   # Crescimento do PIB real (%)
    ]
    data = extract_worldbank_data(countries, indicators)  # Extrai os dados
    extract_logger.info("Processo de extração de dados finalizado")
    return data
