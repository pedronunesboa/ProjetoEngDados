# Projeto MarketPulse: Pipeline de Engenharia de Dados

Este repositório contém um projeto completo de pipeline de engenharia de dados (ELT) de ponta a ponta. O objetivo aqui é simular um ambiente de produção real para a ingestão, processamento e monitoramento de dados do mercado financeiro (ações e notícias), utilizando uma stack de ferramentas moderno e totalmente orquestrado.

O projeto é 100% "containerizado" usando Docker, permitindo total portabilidade e reprodutibilidade.

🚀 Conceito do Projeto
O MarketPulse captura dois tipos de dados de fontes distintas:

Dados Estruturados: Cotações diárias de ações (OHLCV - Open, High, Low, Close, Volume) da B3 (ex: PETR4) através da API Alpha Vantage.

Dados Não Estruturados: Manchetes de notícias do mercado financeiro, coletadas via web scraping do portal InfoMoney e armazenadas em um banco MongoDB.

O objetivo final é mover esses dados brutos através de um Data Lake (AWS S3) e transformá-los em tabelas limpas e agregadas, prontas para consumo analítico e de Business Intelligence.

🏛️ Arquitetura e Fluxo de Dados
O pipeline segue a arquitetura Medallion (Bronze, Silver, Gold), orquestrada pelo Apache Airflow.

1. Fontes de Dados (Source)
API de Ações: Alpha Vantage (Dados estruturados).

MongoDB: Populado por um script de web scraping (BeautifulSoup + requests) que busca notícias do InfoMoney (Dados não estruturados).

2. Orquestração (Apache Airflow)
O coração do projeto, rodando em Docker (com CeleryExecutor, Redis e Postgres). Ele gerencia dois pipelines principais:

Pipeline I (marketpulse_data_ingestion): DAG de ingestão principal (ELT), agendada diariamente.

extract_stocks_to_bronze (DockerOperator): Uma task que roda uma imagem Docker customizada para buscar dados da API de ações e salvá-los como JSON bruto na Camada Bronze (AWS S3).

extract_news_to_bronze (PythonOperator): Uma task (em paralelo) que lê os dados de notícias do MongoDB (via pymongo) e salva o JSON bruto na Camada Bronze (AWS S3).

# Nova feature
transform_bronze_to_gold (DockerOperator): A etapa de transformação (T). Após a ingestão, esta task dispara um contêiner Apache Spark (bitnami/spark) que executa um script PySpark.

Pipeline II (weekly_source_volume_monitoring): DAG de monitoramento e Data Quality, agendada semanalmente.

get_api_volume / get_mongodb_volume (PythonOperators): Tasks que se conectam diretamente às fontes (API e Mongo) para contar o número de registros disponíveis na origem.

store_volume_metrics (PostgresHook): Armazena essas contagens em um banco de dados PostgreSQL de metadados, permitindo o acompanhamento da volumetria e detecção de anomalias.

3. Ingestão (Camada Bronze - AWS S3)
Os dados brutos (JSONs da API e do Mongo) são armazenados no AWS S3 sem modificação, particionados por data e tipo de dado (ex: s3://.../stock_data/ e s3://.../news_data/).

4. Transformação (Spark - Camadas Silver e Gold)
O Objetivo aqui era utilizar os recursos do Databricks para ler os arquivos JSON da camada bronze, transformá-los e salvá-los de volta no S3 em camadas otimizadas (Silver e Gold), porém com mudanças da versão Community Edition para o novo Free Tier do databricks, obtive limitações nas permissões para acessar os dados no S3, sendo assim foi aplicado uma solução robusta que é rodar o Spark localmente, dentro do ambiente Docker.

Foi construído um script PySpark (orquestrado pelo Airflow) para ser responsável por todo o processamento:
Bronze -> Silver: Lê os JSONs brutos do S3, aplica schemas, limpa (trata nulos, ajusta tipos de dados) e salva os dados em formato colunar otimizado (Parquet) na Camada Silver (S3).

Silver -> Gold: Lê os dados limpos da Camada Silver e cria tabelas de negócios, agregadas e prontas para o consumo. Ex: Resumo semanal de ações, Contagem de notícias por dia.

5. Módulo de Demonstração (Databricks)
Como prova de conceito separada, essa etapa inclui um notebook (.py ou .ipynb) para ser executado no Databricks Free Tier.

Ele demonstra como a mesma lógica de transformação (Bronze -> Gold) pode ser executada nativamente na plataforma Databricks, lendo dados do DBFS (via upload manual) e salvando-os como Tabelas Delta Lake, que são então consultadas via Databricks SQL.

🛠️ Tecnologias Utilizadas
Orquestração: Apache Airflow (via Docker Compose)

Bancos de Dados: PostgreSQL (Metastore do Airflow e Metadados do Projeto), MongoDB (Fonte NoSQL)

Processamento de Dados: Apache Spark (PySpark)

Armazenamento (Data Lake): AWS S3

Contêineres: Docker & Docker Compose

Bibliotecas Python: pymongo, boto3, requests, beautifulsoup4

Prova de Conceito (Cloud): Databricks (DBFS, Delta Lake, Databricks SQL)

## Pré-requisitos

1.  Conta na AWS (com chaves de Acesso e Secreta)
2.  Um bucket S3 na AWS (anote o nome)
3.  Conta na [Alpha Vantage](https://www.alphavantage.co/) (anote a API Key)
4.  Conta no [Databricks Free Tier](https://databricks.com/try-databricks)
5.  Docker e Docker Compose instalados

## 1. Configuração do Ambiente

1.  Clone este repositório: `git clone ...`
2.  Navegue até a pasta de configuração: `cd PROJETOENGDADOS/airflow-environment`
3.  Crie seu arquivo `.env` a partir do exemplo: `cp .env.example .env`
4.  Edite o arquivo `.env` e preencha as senhas `METADATA_DB_PASSWORD` e `MONGO_PASSWORD`.

## 2. Build da Imagem de Extração

A DAG de ingestão usa uma imagem Docker customizada. Você precisa "buildar" ela localmente:

1.  Navegue até a pasta do projeto: `cd PROJETOENGDADOS/marketpulse_project`
2.  Execute o build: `docker build -t marketpulse-extractor:latest .`

## 3. Subindo o Ambiente Airflow

1.  Volte para a pasta do Airflow: `cd ../airflow-environment`
2.  Suba os contêineres: `docker-compose up -d --build`
3.  Aguarde alguns minutos e acesse o Airflow em `http://localhost:8080` (usuário/senha padrão: `airflow`/`airflow`).

## 4. Configuração Pós-Subida (Airflow e Databricks)

Você precisa configurar o Airflow e o Databricks manualmente:

### No Airflow (localhost:8080):

1.  **Variáveis:** Vá em `Admin -> Variables` e crie:
    * `aws_access_key_id`: (Sua chave de acesso AWS)
    * `aws_secret_access_key`: (Sua chave secreta AWS)
    * `alpha_vantage_api_key`: (Sua chave da Alpha Vantage)
2.  **Conexões:** Vá em `Admin -> Connections -> +` e crie:
    * **Conn Id:** `mongo_marketpulse_db`
    * **Conn Type:** `Generic`
    * **Host:** `mongo`
    * **Login:** `mongoadmin`
    * **Password:** (A senha que você definiu no `.env` para `MONGO_PASSWORD`)
    * **Port:** `27017`
    * **Extra:** `{"database": "marketpulse_news"}`

### No Databricks:

1.  Crie um novo Notebook (ex: `NotebookMarketPulse`).
2.  Copie o código do arquivo `databricks_notebook.py` (você deve criar este arquivo no seu projeto) e cole no notebook.
3.  Execute a célula de criação dos Widgets.
4.  Execute a célula de teste de leitura, preenchendo os Widgets com suas chaves AWS.

## 5. Execução

1.  No Airflow, ative (unpause) as DAGs `marketpulse_data_ingestion` e `weekly_source_volume_monitoring`.
2.  Dispare a `marketpulse_data_ingestion` manualmente para popular a Camada Bronze.
3.  Execute o notebook Databricks para processar os dados (Bronze -> Silver -> Gold).
