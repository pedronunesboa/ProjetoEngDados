import pendulum
import requests
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pymongo import MongoClient, errors

# Definindo o ID da conexão do postgress que foi criado no airflow
POSTGRES_CONN_ID = "marketpulse_metadata_db"
MONGO_CONN_ID = "mongo_marketpulse_db"      # Conexão genérica com o mongo
STOCK_SYMBOL = 'PETR4.SA'
BASE_URL = 'https://www.alphavantage.co/query'

@dag(
    dag_id="weekly_source_volume_monitoring",
    schedule_interval = "@weekly", # Conforme o plano, roda mensalmente
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    doc_md="""
    ### MarketPulse Source Volume Monitoring DAG
    Esta DAG consulta e armazena os dados do mercado de ações.
    - Conta os registros da fonte API e MongoDB *
    - Armazena as contagens na tabela de metadados no PostgreSQL
    """,
    tags=["projeto_marketpulse", "pipeline_2", "monitoramento"]
)
def weekly_source_volume_monitoring_dag():
    """
    DAG - Pilepine II: Acompanhamento Semanal de volumetria (source)
    
    Esta DAG monitora a voluemtria de dados nas fontes originais
    (API de ações e mongo db de notícias) e armazena essas
    métricas em uma tabela de metadados no PostgreSQL.
    """

    @task
    def get_api_volume(**kwargs):
        """ 
        Task 1: Busca os dados da API alpha vantage e conta os registros

        Esta task usa a airflow variable 'alpha_vantage_api_key' para se autenticar.
        """
        print(f"Buscando dados reais da API para {STOCK_SYMBOL}...")

        try:
            # Pega a API Key e armazena de forma segura no Airflow
            api_key = Variable.get("alpha_vantage_api_key")
        except:
            print("Erro ao tentar encontrar a variável 'alpha_vantage_api_key' no airflow.")
            raise
        
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': STOCK_SYMBOL,
            'outputsize': 'compact', # compact retorna os últimos 100 pontos
            'apikey': api_key
        }

        volume_count = 0
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if "Error Message" in data:
                print(f"Erro da API: {data['Error Message']}")
                raise ValueError(f"Erro da API: {data['Error Message']}")
            
            if "Time Series (Daily)" in data:
                # Esta é a métrica que conta quantos dias retornaram.
                volume_count = len(data["Time Series (Daily)"])
                print(f"Busca concluída. Número de registros (dias): {volume_count}")
            else:
                print("Resposta da API não contém 'Time Series (Daily)'.")
                print(f"Resposta recebida: {data}")
                # Se a API não retornar dados (ex: limite atingido), contagem = 0
                volume_count = 0

        except Exception as e:
            print(f"Ocorreu um erro ao chamar a API: {e}")
            raise
        
        logical_date = kwargs['logical_date']

        return {
            "data_referencia":str(logical_date.date()),
            "volume": volume_count
        }
    
    @task
    def get_mongodb_volume(**kwargs):
        """
        Task 2: Busca a volumetria REAL de notícias do MongoDB.

        Usa a conexão genérica 'mongo_marketpulse_db' e pymongo
        para se conectar e contar os documentos.
        """
        print("Buscando volumetria real do MongoDB...")
        
        try:
            #1. Ler a conexão genérica 
            conn = BaseHook.get_connection(MONGO_CONN_ID)

            #2. Construir a string de conexão (lembrar de authSource=admin)
            connection_string = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/?authSource=admin"

            #3. Conectar ao mongo
            client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)  #Timeout de 5s

            #4. Selecionar o DB (do campo extra) e a collection
            db_name = conn.extra_dejson.get('database', 'marketpulse_news')
            db = client[db_name]
            collection = db['noticias'] # Nome da coleção utilizada pelo scraper

            #5. Contagem simples de documentos
            volume_count = collection.count_documents({})
            print(f"Contagem do MongoDB: {volume_count} notícias.")

            client.close()
        
        except Exception as e:
            print(f"Erro ao conectar ou contar documentos no MongoDB: {e}")
            raise   # Faz a task falhar se não conseguir conectar

        #6. Retorn os dados para o XCom (igual a antiga task simulada)
        logical_date = kwargs['logical_date']
        return {
            "data_referencia": str(logical_date.date()),
            "volume": volume_count
        }
    
    @task
    def store_volume_metrics(api_metrics: dict, mongo_metrics: dict):
        """
        Task 3: Armazena as métricas no banco de metaos PostgresSQL.
        """

        print("Iniciando armazenamento de métricas no PostgreSQL...")

        sql_insert = """
        INSERT INTO volume_metrics (data_referencia, fonte, volumetria_registros)
        VALUES (%s, %s, %s)
        """

        data_ref_api = api_metrics['data_referencia']
        volume_api = api_metrics['volume']

        data_ref_mongo = mongo_metrics['data_referencia']
        volume_mongo = mongo_metrics['volume']

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            hook.run(sql_insert, parameters=(data_ref_api, 'api_acoes', volume_api))
            print(f"Métrica da API inserida: {data_ref_api}, 'api_acoes', {volume_api}")

            hook.run(sql_insert, parameters=(data_ref_mongo, 'mongodb_noticias', volume_mongo))
            print(f"Métrica do MongoDB inserida: {data_ref_mongo}, mongodb_noticias, {volume_mongo}")
            
            print("Métricas armazenadas com sucesso!")
        
        except Exception as e:
            print(f"Erro ao inserir métricas no PostgreSQL: {e}")
            raise

    # --- Definindo o fluxo da DAG ---
    api_data = get_api_volume()
    mongo_data = get_mongodb_volume()

    store_volume_metrics(api_metrics=api_data, mongo_metrics=mongo_data)

# Necessário para o Airflow "descobrir" a DAG
weekly_source_volume_monitoring_dag()