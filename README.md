# 🌍 World Bank Data ETL & Analytics Pipeline

## 🔹 Sobre o Projeto

Este projeto é um **pipeline ETL completo** para extração, transformação e carga de indicadores econômicos e sociais de diversos países a partir da API do **World Bank**. Além do ETL, inclui **visualizações e análises** dos dados, demonstrando **pipeline de dados, modelagem dimensional e storytelling com dados**.  

O projeto é containerizado com **Docker e Docker Compose**, permitindo execução reproduzível e escalável em qualquer ambiente.

---

## 🏗 Arquitetura do Pipeline

O pipeline segue a arquitetura ETL modular:

1. **Extract** (`src/extract/extract.py`):
   - Consulta a API do World Bank.
   - Extrai indicadores econômicos e sociais para países definidos.
   - Logging detalhado de cada requisição e erro.

2. **Transform** (`src/transform/transform.py`):
   - Converte JSON/lista de dicionários em DataFrame do Pandas.
   - Converte tipos de dados e normaliza colunas.
   - Remove valores nulos e loga dados removidos.
   
3. **Load** (`src/load/load.py`):
   - Conecta ao **PostgreSQL** usando variáveis de ambiente.
   - Cria tabelas dimensionais (`dim_country`, `dim_indicator`) e tabela fato (`fact_indicators`) caso não existam.
   - Insere dados transformados usando `execute_values` para **bulk insert** com tratamento de duplicidade (`ON CONFLICT DO NOTHING`).
   
4. **Orquestração** (`src/main.py`):
   - Executa pipeline completo: Extract → Transform → Load.
   - Executa notebooks de visualização com **Papermill**.
   - Logging de início, progresso e finalização do pipeline.
   - Tratamento de erros críticos com rastreabilidade.

---

## 📊 Análises e Visualizações

O pipeline gera insights como:

- Evolução do **PIB (USD)** por país ao longo do tempo.
- Evolução da **taxa de desemprego (%)** por país.
- **Expectativa de Vida vs PIB per capita** (scatter plot) para análise de correlação.
- Crescimento econômico médio (%) por país (gráfico de barras).

As visualizações são feitas com **Matplotlib** e preparadas para integração em notebooks interativos.

---

## 🗂 Estrutura de Pastas

```
.
├── docker-compose.yml        # Configuração dos containers (PostgreSQL + Python)
├── Dockerfile                # Container Python para pipeline ETL
├── requirements.txt          # Dependências Python
├── docs/
├── notebooks/                # Notebooks de análise e visualização
├── src/
│   ├── main.py               # Orquestração do pipeline
│   ├── extract/
│   │   └── extract.py        # Extração da API World Bank
│   ├── transform/
│   │   └── transform.py      # Transformação de dados
│   └── load/
│       └── load.py           # Conexão e carga no PostgreSQL
└── README.md
```



## ⚙️ Tecnologias Utilizadas

- Python – processamento de dados, ETL, automação.

- Pandas – manipulação e transformação de dados.

- PostgreSQL – armazenamento de dados estruturados (DW estilo Star Schema).

- Papermill – execução de notebooks automaticamente após ETL.

- Docker & Docker Compose – containerização de pipeline e banco de dados.

- Matplotlib – visualizações estáticas e plots de análise.

- Logging – rastreabilidade completa do pipeline.


## 🚀 Como Executar o Projeto

Clone o repositório:

```
git clone https://github.com/seu-usuario/worldbank-etl.git  
```
```
cd worldbank-etl
```

Suba os containers com Docker Compose:

```
docker-compose up --build
```

O container db executa PostgreSQL.

O container python executa o pipeline ETL e notebooks.


Configuração de variáveis de ambiente (opcional):

```
DB_HOST=db  
DB_NAME=world_bank  
DB_USER=postgres  
DB_PASSWORD=postgres  
DB_PORT=5432  
```

Pipeline ETL será executado automaticamente via CMD do Dockerfile:

```
python -m src.main
```



## 📌 Skills Demonstradas

- Engenharia de Dados: construção de pipelines ETL robustos e escaláveis.

- Modelagem Dimensional: criação de tabelas fato e dimensões (Star Schema).

- Data Cleaning & Transformation: tratamento de dados, tipos, valores nulos.

- Visualização e Analytics: geração de insights via gráficos e análise de tendências.

- DevOps / Containerização: Docker + Docker Compose para ambiente reproduzível.

- Automação com Notebooks: integração ETL + visualização via Papermill.

- Boas práticas: logging, tratamento de erros, type hints e modularidade.



## 📈 Conclusão

Este projeto demonstra a capacidade de projetar, implementar e orquestrar pipelines de dados profissionais, integrando ETL, análise e visualização, além de containerização e boas práticas de engenharia.