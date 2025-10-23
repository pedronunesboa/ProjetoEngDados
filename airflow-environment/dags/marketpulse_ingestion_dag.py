from __future__ import annotations

import pendulum
import logging
import json
from datetime import datetime
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

# --- Constantes para a nova task ---
MONGO_CONN_ID = "mongo_marketpulse_db"
# Vindo do script extract.py original
S3_BUCKET_NAME = 'marketpulse-bronze-layer-pedroboa-20251006'

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
    doc_md="""
    ### MarketPulse Data Ingestion DAG
    Esta DAG orquestra a ingestão de dados para a camada Bronze.
    - **Task 1 (Docker):** Extrai dados de Ações (API) e salva no S3.
    - **Task 2 (Python):** Extrai dados de Notícias (MongoDB) e salva no S3.
    - As tasks rodam em paralelo.
    """,
    tags=["marketpulse", "ingestion", "bronze", "pipeline_1"],
) as dag:
    # --- Task 1: Extração de ações (API -> S3)
    # --- Definição da Tarefa ---
    extract_task_stocks = DockerOperator(
        task_id="extract_stocks_to_bronze",
        image="marketpulse-extractor:latest", # Nome da imagem que construímos
        auto_remove=True,

        # Agora estamos passando o dicionários que lemos das Variables (Método atual)
        environment=env_vars_from_airflow,

        # Garante que o container consegue se comunicar com o Docker Engine do host
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
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
    
    # Fim da nova task

    # --- Orquestração ---
    # Instancia a nova task. Como ela não tem dependencias (>> ou <<)
    # com a 'extract_task_stocks', o Airflow irá executá-las em paralelo.
    extract_news_to_bronze()
