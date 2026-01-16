from __future__ import annotations

import pendulum
import logging
import json
from datetime import datetime, timedelta
from bson import json_util # Para converter ObjectID e ISODate do Mongo

import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient, errors
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException # Importa exceção padrão
from airflow.utils.task_group import TaskGroup     # Importação adicionada, necessária para agrupar tasks (TaskGroups)

# --- Novas importações para a Task de Carga no BI ---
import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable

# --- Constantes para a nova task ---
MONGO_CONN_ID = "mongo_marketpulse_db"
POSTGRES_CONN_ID = "marketpulse_metadata_db"
# Vindo do script extract.py original
S3_BUCKET_NAME = 'marketpulse-bronze-layer-pedroboa-20251006'

# Lista de ações para buscar na API
STOCK_LIST = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA",
              "WEGE3.SA", "ABEV3.SA", "MGLU3.SA", "EMBJ3.SA", "FLRY3.SA"]

# --- Etapa 1: Puxar variáveis (Segurança) ---
# Puxa as variáveis ANTES de definir a DAG.
# Se alguma variável estiver faltando, a DAG nem será carregada (o que é bom).
try:
    env_vars_from_airflow = {
        "AWS_ACCESS_KEY_ID": Variable.get("aws_access_key_id"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("aws_secret_access_key"),
        "AWS_DEFAULT_REGION": Variable.get("aws_default_region"),
        "ALPHA_VANTAGE_API_KEY": Variable.get("alpha_vantage_api_key")
    }
except KeyError as e:
    # Lanã um erro claro se uma variável estiver faltando no aiflow
    raise AirflowException(f"ERRO: A variável {e} não foi definida. Por favor, adicione-a em Admin -> Variables.")

# --- Definição da DAG ---
with DAG(
    dag_id="marketpulse_data_ingestion",
    start_date=pendulum.datetime(2025, 10, 15, tz="America/Sao_Paulo"),
    schedule="@daily", # Executa uma vez por dia, logo após a meia-noite
    catchup=False,
    default_args={
        'retries': 3, # Tenta até 3 vezes se a API falhar
        'retry_delay': timedelta(minutes=2), # Espera 2 min entre tentativas
    },
    doc_md="""
    ### Pipeline ELT Marketpulse (Bronze -> Gold)
    Esta DAG orquestra o pipeline completo de ingestão e transformação.
    - E (Extract): Tasks 1 e 2 rodam em paralelo para extrair dados da API e do Mongo
    - L (Load): As mesmas tasks salvam os dados brutos no S3 (camada bronze).
    - T (Transform): Task 3 roda o job Spark (em docker) para transformar os dados da
    camada Bronze para as camadas Silver/Gold.
    """,
    tags=["projeto_marketpulse", "spark", "elt", "multi_stocks"],
) as dag:
    # --- Task 1: Extração de ações (API -> S3)
    # --- Definição da Tarefa ---
    with TaskGroup("extract_stocks_group", tooltip="Extrai lista de ações sequencialmente") as extract_stocks_group:

        for stock in STOCK_LIST:
            # Tratamento do nome para o ID da task (Airflow não aceita pontos no ID)
            clean_stock_id = stock.replace(".", "_")

            # Criamos uma nova cópia das variáveis e injvetamos o símbolo específico dessa iteração
            current_env_vars = env_vars_from_airflow.copy()
            current_env_vars['STOCK_SYMBOL'] = stock

            DockerOperator(
                task_id=f'extract_{clean_stock_id}', # Ex: extract_PETR4_SA
                image="marketpulse-extractor:latest",
                container_name=f'task_extract_{clean_stock_id}', # Facilita debug no Docker
                auto_remove=True,

                # O segredo do rate limit ---
                pool = 'alpha_vantage_pool', # Usa a pool criada para limitar concorrência

                environment=current_env_vars,
                docker_url="unix://var/run/docker.sock",
                network_mode = "bridge",
                mount_tmp_dir=False
            )

    # --- Task 2: Extração de notícias (Mongo -> S3)
    @task
    def extract_news_to_bronze():
        """
        Extrai todas as notpicias da coleçao 'noticias' do MongoDB
        e salva como um único arquivo JSON na camada bronze (S3)
        """
        logging.info("Iniciando extração de notícias do MongoDB")

        # 1. Conectar ao MongoDB
        try:
            logging.info(f"Lendo conexão genérica: {MONGO_CONN_ID}")
            conn = BaseHook.get_connection(MONGO_CONN_ID)
            connection_string = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/?authSource=admin"
            client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)

            db_name = conn.extra_dejson.get('database', 'marketpulse_news')
            db = client[db_name]
            collection = db['noticias']

            # Buscar os dados
            logging.info(f"Buscando documentos da coleção 'noticias' no DB '{db_name}'...")
            noticias_cursor = collection.find({})
            noticias_list = list(noticias_cursor) # Converte o cursor para uma lista
            client.close()

            if not noticias_list:
                logging.warning("Nenhuma notícia encontrada no MongoDB. Pulando upload para S3.")
                return
            
            logging.info(f"Encontradas {len(noticias_list)} noticias. Convertendo para JSON...")

            # Converte a lista para JSON (usando json_util para BSON types)
            # isso lida corretamente com ObjectId() e ISODate()
            json_data = json_util.dumps(noticias_list, indent=4)
        except Exception as e:
            logging.error(f"Erro ao conectar ou buscar dados no MongoDB: {e}")
            raise

        # 3. Conectar ao S3 e fazer upload
        try:
            logging.info("Conectando ao S3...")
            # pega as credenciais das variáveis já carregadas
            s3_client = boto3.client(
                's3',
                aws_access_key_id=env_vars_from_airflow["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=env_vars_from_airflow["AWS_SECRET_ACCESS_KEY"],
                region_name=env_vars_from_airflow["AWS_DEFAULT_REGION"]
            )

            # Define o nome e o caminho do arquivo no S3
            current_date = datetime.now().strftime('%Y-%m-%d')
            file_name = f"infomoney_news_{current_date}.json"
            s3_key = f"news_data/{file_name}" # Salva em uma "pasta" separada

            logging.info(f"Salvando dados no S3: s3://{S3_BUCKET_NAME}/{s3_key}")

            s3_client.put_object(
                Body=json_data,
                Bucket=S3_BUCKET_NAME,
                Key=s3_key
            )

            logging.info("Upload de notícias para o S3 concluído com sucesso!")

        except ClientError as e:
            logging.error(f"Erro (ClientError) ao salvar no S3: {e}")
            raise
        except Exception as e:
            logging.error(f"Erro inesperado ao salvar no S3: {e}")
            raise
    
    # --- Instanciação da Task 2 ---
    # Aqui vamos "chamar" a função para que ela se torne uma task
    extract_news_to_bronze_task = extract_news_to_bronze()

    # --- Task 3 (NOVA): Transformaçõ Spark (S3 bronze -> S3 gold)
    # 1. Copiando o dicionário de ambiente principal

    transform_bronze_to_gold = DockerOperator(
        task_id = "transform_bronze_to_gold",
        image = "marketpulse-transformer:latest", # <-- A imagem que acabamos de buildar 
        auto_remove = True,
        environment = env_vars_from_airflow, # <-- Passa as chaves da aws
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge" # <-- Usa a rede 'bridge' para acesso a internet para baixar jars do S3
    )

    # --- Task 4 (NOVA): Carga do S3 Gold -> Postgres Datalab ---
    @task
    def load_gold_to_postgres():
        """
        Lê as tabelas da Camada Gold (Delta Lake) do S3 e as carrega no banco de dados PostgreSQL
        para consumo do BI
        """
        logging.info("Iniciando carga S3 Gold -> Postgres...")

        # 1. Definir caminhos s3 gold 
        gold_agg_news_path = f"s3://{S3_BUCKET_NAME}/gold/agg_noticias_por_dia"
        gold_agg_stocks_path = f"s3://{S3_BUCKET_NAME}/gold/agg_acoes_semanal/"

        #opções de armazenamento para a bilbioteca 'deltalake' ler do S3.
        storage_options = {
            "aws_access_key_id": env_vars_from_airflow["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": env_vars_from_airflow["AWS_SECRET_ACCESS_KEY"],
            "aws_region": env_vars_from_airflow["AWS_DEFAULT_REGION"],
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true" # Permite renomear arquivos no S3
        }

        # --- 2. Conectar ao Postgres ---
        try:
            logging.info(f"Lendo conexão Postgres: {POSTGRES_CONN_ID}")
            conn_pg = BaseHook.get_connection(POSTGRES_CONN_ID)
            # Formato: postgresql://[user]:[password]@[host]:[port]/[database]
            conn_string_pg = (
                f"postgresql://{conn_pg.login}:{conn_pg.password}@"
                f"{conn_pg.host}:{conn_pg.port}/{conn_pg.schema}"
            )
            engine = create_engine(conn_string_pg)
            logging.info("Conexão com Postgres (SQLAlchemy) criada com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao criar conexão com Postgres: {e}")
            raise

        # --- 3. Ler Tabela Gold (Ações) e Carregar no Postgres ---
        try:
            logging.info(f"Lendo tabela Delta: {gold_agg_stocks_path}")
            dt_stocks = DeltaTable(gold_agg_stocks_path, storage_options=storage_options)
            df_stocks = dt_stocks.to_pandas()
            
            logging.info(f"Carregando {len(df_stocks)} linhas na tabela 'gold_agg_acoes_semanal'...")
            df_stocks.to_sql(
                "gold_agg_acoes_semanal",
                engine,
                if_exists="replace",
                index=False
            )
            logging.info("Tabela 'gold_agg_acoes_semanal' carregada com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao processar tabela de ações: {e}")
            raise

        # --- 4. Ler Tabela Gold (Notícias) e Carregar no Postgres ---
        try:
            logging.info(f"Lendo tabela Delta: {gold_agg_news_path}")
            dt_news = DeltaTable(gold_agg_news_path, storage_options=storage_options)
            df_news = dt_news.to_pandas()
            
            logging.info(f"Carregando {len(df_news)} linhas na tabela 'gold_agg_noticias_por_dia'...")
            df_news.to_sql(
                "gold_agg_noticias_por_dia",
                engine,
                if_exists="replace",
                index=False
            )
            logging.info("Tabela 'gold_agg_noticias_por_dia' carregada com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao processar tabela de notícias: {e}")
            raise

        logging.info("Carga S3 Gold -> Postgres concluída com sucesso.")

    # --- Instanciação da Task 4 ---
    load_gold_to_postgres_task = load_gold_to_postgres()
    
    # ===================================================================
    # --- FIM DA NOVA TASK ---
    # ===================================================================

    # --- Orquestração ---
    # Configura a Task 3 para rodar apenas depois que as tasks 1 e 2 terminarem com sucesso.
    # ELT -> Load to BI
    [extract_stocks_group, extract_news_to_bronze_task] >> transform_bronze_to_gold >> load_gold_to_postgres_task

