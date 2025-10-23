import requests
import json
import os
import sys
import boto3 # Biblioteca da AWS para Python
from botocore.exceptions import ClientError
from datetime import datetime

# --- Configuração ---
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
STOCK_SYMBOL = 'PETR4.SA'
BASE_URL = 'https://www.alphavantage.co/query'
S3_BUCKET_NAME = 'marketpulse-bronze-layer-pedroboa-20251006' # <-- IMPORTANTE: SUBSTITUA PELO NOME DO SEU BUCKET

def fetch_stock_data(api_key, symbol):
    """Busca os dados históricos diários de uma ação."""
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'compact',
        'apikey': api_key
    }
    print(f"Buscando dados para o símbolo: {symbol}...")
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if "Error Message" in data:
            print(f"Erro da API: {data['Error Message']}")
            return None
        print("Busca de dados concluída com sucesso!")
        return data
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return None

def save_to_s3(data, bucket, symbol):
    """Salva os dados em um arquivo JSON no S3."""
    # Gera um nome de arquivo dinâmico com a data e hora atuais
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_name = f"{symbol}_{current_date}.json"
    
    # Estrutura de pastas dentro do bucket (boa prática)
    s3_key = f"stock_data/symbol={symbol}/{file_name}"

    print(f"Salvando dados no S3: s3://{bucket}/{s3_key}")
    
    try:
        s3_client = boto3.client('s3')
        # Converte o dicionário Python para uma string JSON
        json_string = json.dumps(data, indent=4)
        
        # Faz o upload do objeto
        s3_client.put_object(Body=json_string, Bucket=bucket, Key=s3_key)
        
        print("Dados salvos com sucesso no S3!")
        return True
    except ClientError as e:
        print(f"Erro ao salvar no S3: {e}")
        return False

# --- Bloco de Execução Principal ---
if __name__ == "__main__":
    if not API_KEY:
        print("ERRO: A variável de ambiente 'ALPHA_VANTAGE_API_KEY' não foi definida.")
        sys.exit(1)
    
    #if 'pedroboa-20251006' in S3_BUCKET_NAME:
    #    print("ERRO: Por favor, substitua o nome do bucket S3 na variável S3_BUCKET_NAME.")
    #    sys.exit(1)

    # 1. Busca os dados da API
    stock_data = fetch_stock_data(API_KEY, STOCK_SYMBOL)
    
    # 2. Se a busca foi bem-sucedida, salva no S3
    if stock_data:
        save_to_s3(data=stock_data, bucket=S3_BUCKET_NAME, symbol=STOCK_SYMBOL)