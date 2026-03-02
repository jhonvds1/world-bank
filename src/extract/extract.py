import requests


def extract_worldbank_data(countries, indicators):
    data = []
    for country in countries:
        for indicator in indicators:
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json"
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                json_data = response.json()
            
                if len(json_data) > 1 and json_data is not None:
                    for record in json_data[1]:
                        data.append({
                            "country": record["country"]["value"],
                            "country_id": record["country"]["id"],
                            "indicator": record["indicator"]["value"],
                            "indicator_id": record["indicator"]["id"],
                            "year": record["date"],
                            "value": record["value"]
                        })
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar {country} - {indicator}: {e}")
    return data



def run_extract():
    countries = ["BRA", "USA", "CHN", "IND", "DEU"]
    indicators = [
        "NY.GDP.MKTP.CD", # PIB total do país em dólares atuais (sem ajuste de inflação).
        "NY.GDP.PCAP.CD", # PIB dividido pela população → mede riqueza média por pessoa.
        "SP.POP.TOTL", # Quantidade total de habitantes do país.
        "SP.DYN.LE00.IN", # Quantos anos, em média, uma pessoa recém-nascida deve viver.
        "SL.UEM.TOTL.ZS", #  Percentual da população economicamente ativa que está desempregada.
        "NY.GDP.MKTP.KD.ZG" # Taxa de crescimento do PIB (já ajustada pela inflação, crescimento real).
    ]
    data = extract_worldbank_data(countries, indicators)

    return data
