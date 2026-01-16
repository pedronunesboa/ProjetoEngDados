# Projeto MarketPulse: Pipeline de Engenharia de Dados

Este repositório contém um projeto completo de pipeline de engenharia de dados (ELT) de ponta a ponta. O objetivo é simular um ambiente de produção real para a ingestão, processamento, monitoramento e visualização de dados do mercado financeiro (ações e notícias), utilizando uma stack de ferramentas modernas e totalmente orquestrada.

O projeto é 100% "containerizado" usando Docker, permitindo total portabilidade e reprodutibilidade.

## 🚀 Conceito do Projeto
O **MarketPulse** captura dois tipos de dados de fontes distintas:

1.  **Dados Estruturados:** Cotações diárias de ações (OHLCV - Open, High, Low, Close, Volume) da B3 (ex: PETR4) através da API Alpha Vantage.
2.  **Dados Não Estruturados:** Manchetes de notícias do mercado financeiro, coletadas via web scraping do portal InfoMoney e armazenadas em um banco MongoDB.

O objetivo final é mover esses dados brutos através de um Data Lake (AWS S3), transformá-los com Spark e disponibilizá-los em um Data Warehouse (PostgreSQL) para consumo analítico no Power BI.

## 🏛️ Arquitetura e Fluxo de Dados
O pipeline segue a arquitetura Medallion (Bronze, Silver, Gold), orquestrada pelo Apache Airflow.

**1. Fontes de Dados (Source)**
* **API de Ações:** Alpha Vantage (Dados estruturados).
* **MongoDB:** Populado por um script de web scraping customizado (BeautifulSoup + requests) que busca notícias do InfoMoney.

**2. Orquestração (Apache Airflow)**
O coração do projeto, rodando em Docker. Ele gerencia dois pipelines principais:

* **Pipeline I (`marketpulse_data_ingestion`):** DAG principal de ELT, agendada diariamente.
    * `extract_stocks_to_bronze` (DockerOperator): Roda uma imagem Docker customizada para buscar dados da API e salvá-los na Camada Bronze (S3).
    * `extract_news_to_bronze` (PythonOperator): Lê notícias do MongoDB (via pymongo) e salva o JSON bruto na Camada Bronze (S3).
    * `transform_bronze_to_gold` (DockerOperator): A etapa de transformação (T). Dispara um contêiner **Apache Spark** (imagem oficial `apache/spark`) que executa um job PySpark para processar os dados Bronze -> Silver -> Gold.
    * `load_gold_to_postgres` (PythonOperator): A etapa final de carga. Lê as tabelas Delta Lake da camada Gold no S3 e as carrega no banco de dados PostgreSQL (Datalab) para consumo.

* **Pipeline II (`weekly_source_volume_monitoring`):** DAG de monitoramento e Data Quality.
    * `get_api_volume` / `get_mongodb_volume`: Conectam-se às fontes originais para aferir a volumetria real.
    * `store_volume_metrics`: Armazena metadados de volumetria no PostgreSQL para auditoria.

**3. Ingestão (Camada Bronze - AWS S3)**
* Os dados brutos (JSONs) são armazenados no AWS S3 sem modificação, particionados por data (ex: `s3://.../stock_data/` e `s3://.../news_data/`).

**4. Transformação (Spark - Camadas Silver e Gold)**
Utilizamos o **Apache Spark** rodando localmente em contêiner Docker para garantir controle total sobre as dependências e acesso ao S3. O script PySpark realiza:
* **Bronze -> Silver:** Leitura dos JSONs (com tratamento de schemas complexos via `stack`/`explode`), limpeza de dados, tipagem e salvamento em formato **Delta Lake** no S3.
* **Silver -> Gold:** Agregação de dados para regras de negócio (ex: Média semanal de fechamento, Contagem de notícias por categoria) salvos em **Delta Lake**.

**5. Camada de Serviço e Visualização (PostgreSQL + Power BI)**
* **Data Warehouse (PostgreSQL):** As tabelas da camada Gold são carregadas do S3 para o banco de dados PostgreSQL (`marketpulse_metadata`) usando a biblioteca `deltalake` e `sqlalchemy`.
* **Visualização (Power BI):** O Power BI conecta-se diretamente ao PostgreSQL (modo Import) para alimentar os dashboards de análise de mercado e monitoramento de pipeline.

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow (via Docker Compose)
* **Processamento de Dados:** Apache Spark (PySpark 3.5.1)
* **Armazenamento (Data Lake):** AWS S3 (Formatos JSON, Parquet/Delta Lake)
* **Bancos de Dados:**
    * PostgreSQL (Metastore do Airflow e Data Warehouse/Metadados)
    * MongoDB (Fonte de dados NoSQL)
* **Infraestrutura:** Docker & Docker Compose
* **Linguagem & Bibliotecas:** Python (boto3, pymongo, pandas, sqlalchemy, deltalake, requests, beautifulsoup4)
* **Visualização:** Microsoft Power BI

## Pré-requisitos

1.  Conta na AWS (com chaves de Acesso e Secreta)
2.  Um bucket S3 na AWS (anote o nome)
3.  Conta na [Alpha Vantage](https://www.alphavantage.co/) (anote a API Key)
4.  Docker e Docker Compose instalados e permissões de usuário para gerenciar o docker

## 1. Configuração da Nova Máquina (Permissões do Docker)

Em uma nova máquina Linux/WSL, seu usuário precisa de permissão para gerenciar o Docker.

1.  Adicione seu usuário ao grupo `docker`:
    ```bash
    sudo usermod -aG docker $USER
    ```
2.  **Feche e reabra seu terminal** para que as novas permissões entrem em vigor.
3.  Verifique se funcionou rodando `docker ps`. Se não der erro de permissão, você está pronto.

## 2. Configuração do Ambiente

1.  Clone este repositório: `git clone ...`
2.  Navegue até a pasta de configuração: `cd PROJETOENGDADOS/airflow-environment`
3.  Crie seu arquivo `.env` a partir do exemplo: `cp .env.example .env`
4.  Descubra ser User ID local (provavelmetne `1000`) rodando: `id -u`.
5. Edite o arquivo `.env` e preencha as senhas `POSTGRES_PASSWORD`, `METADATA_DB_PASSWORD`, `MONGO_PASSWORD` e `AIRFLOW_UID` com o número do passo anterior.


## 3. Build das Imagens Docker Customizadas

Nosso pipeline usa duas imagens customizadas. Precisamos "buildar" ambas localmente.

1.  **Build da Imagem de Extração:**
    ```bash
    cd PROJETOENGDADOS/marketpulse_project
    docker build -t marketpulse-extractor:latest .
    ```

2.  **Build da Imagem de Transformação (Spark):**
    ```bash
    cd PROJETOENGDADOS/marketpulse_transform
    docker build -t marketpulse-transformer:latest .
    ```

## 4. Subindo o Ambiente Airflow

1.  Volte para a pasta do Airflow: `cd ../airflow-environment`  

2.  Suba todos os containers com todos os serviços (Airflow, Postgres, Mongo, etc.):
    ```bash
    docker-compose up -d --build
    ```
3.  Aguarde alguns minutos e acesse o Airflow em `http://localhost:8080` (usuário/senha padrão: `airflow`/`airflow`).

## 5. Configuração Pós-Subida (Conexões do Airflow)

Você precisa configurar o Airflow e os Bancos manualmente:

### No Airflow (localhost:8080):

1.  **Variáveis:** Vá em `Admin -> Variables` e crie:
    * `aws_access_key_id`: (Sua chave de acesso AWS)
    * `aws_secret_access_key`: (Sua chave secreta AWS)
    * `alpha_vantage_api_key`: (Sua chave da Alpha Vantage)
    * `aws_default_region`: (Região default da aplicação, ex `us-east`)

2.  **Conexão 1 (MongoDB):** Vá em `Admin -> Connections -> +` e crie:
    * **Conn Id:** `mongo_marketpulse_db`
    * **Conn Type:** `Generic`
    * **Host:** `mongo`
    * **Database:** `marketpulse_news`
    * **Login:** `mongoadmin`
    * **Password:** (A senha que você definiu no `.env` para `MONGO_PASSWORD`)
    * **Port:** `27017`
    * **Extra:** `{"database": "marketpulse_news"}` (Opcional)

3.  **Conexão 2 (Postgres - Datalab):** Vá em `Admin -> Connections -> +` e crie:
    * **Conn Id:** `marketpulse_metadata_db`
    * **Conn Type:** `Postgres`
    * **Host:** `metadata-db`
    * **Database:** `marketpulse_metadata`
    * **Login:** `marketpulse_user`
    * **Password:** (A senha que você definiu no `.env` para `METADATA_DB_PASSWORD`)
    * **Port:** `5432`
    * (Clique em **Test** para verificar se a conexão funciona)


## 6. Execução e Verificaão

1.  No Airflow, ative (unpause) as DAGs `marketpulse_data_ingestion` e `weekly_source_volume_monitoring`.
2.  **Execute o Web Scraper (Primeira vez):** Para popular o MongoDB com dados, rode o script de scraping uma vez manualmente no seu terminal:
    ```bash
    docker exec -it airflow-environment_airflow-worker_1 python /opt/airflow/dags/scrape_infomoney.py
    ```
3.  **Execute o Pipeline ELT:** Dispare a DAG `marketpulse_data_ingestion` manualmente. Esta é a DAG principal que roda todo o pipeline (Bronze -> Silver -> Gold -> Postgres).
4.  **Verifique o Resultado Final:** Após a DAG rodar com sucesso (tudo verde), conecte-se ao banco Postgres para ver suas tabelas prontas para o BI:
    ```bash
    docker exec -it airflow-environment_metadata-db_1 psql -U marketpulse_user -d marketpulse_metadata
    ```
    E então rode os selects:
    ```sql
    SELECT * FROM gold_agg_acoes_semanal LIMIT 5;
    SELECT * FROM gold_agg_noticias_por_dia LIMIT 5;
    \q
    ```
5.  Se os dados aparecerem, o pipeline está completo e pronto para ser conectado ao Power BI!

