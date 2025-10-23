# Projeto MarketPulse: Pipeline de Engenharia de Dados

(Breve descrição do projeto...)

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