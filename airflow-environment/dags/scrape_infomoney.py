import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
from datetime import datetime, UTC
import sys
import re # Importar regex para limpar os links

# --- Cnfiguração de conexão com o MongoDB ---
# Mesmos valores do docker-compose
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_USER = 'mongoadmin'
MONGO_PASS = 'mongopass'
MONGO_DB_NAME = 'marketpulse_news' # nome do banco definido na conexão genérica
MONGO_COLLECTION_NAME = 'noticias'
MONGO_AUTH_DB = 'admin' # Onde  o usuário 'mongoadmin' foi criado

# -- COnfiguração do Scraper ---
URL_INFOMONEY = 'https://www.infomoney.com.br/ultimas-noticias'
# Headers para simular um navegador real
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def connect_to_mongo():
    """ Tentativa de se conectar ao mongoDB"""
    try:
        connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_DB}"
        client = MongoClient(connection_string)

        # Testa a conexão
        client.admin.command('ping')
        print("Conexão com o MongoDB estabelecida com sucesso")

        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        return collection
    except errors.PyMongoError as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None

def scrape_infomoney():
    "Busca as notícias do InfoMoney."
    print(f"Buscando notícias em: {URL_INFOMONEY}....")
    try:
        response = requests.get(URL_INFOMONEY, headers=HEADERS, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Usamos a classe exata encontrada no inspecionar elemento para cada card de notícia.
        noticias_html = soup.find_all('div', class_='basis-1/4 px-6 md:px-0')

        noticias_list = []
        if not noticias_html:
            print("\n!!! AVISO: Nenhum 'card' de notícia encontrado com o seletor principal.")
            print("!!! O layout do site InfoMoney provavelmente mudou (ou o seletor está errado).")
            print("!!! Seletor usado: 'div', class_='basis-1/4 px-6 md:px-0'\n")
            return []

        print(f"Encontrados {len(noticias_html)} 'cards' de notícia. Processando...")

        for item in noticias_html:
            categoria_tag = None
            link_tag = None
            titulo_texto = None
            
            # 1. Encontra o BLOCO da Categoria, como você identificou
            categoria_block = item.find('div', class_='line-clamp-1')
            if categoria_block:
                # Dentro dele, procura a tag de texto da categoria
                categoria_tag = categoria_block.find('div', class_=re.compile(r'!text-wl-brand-text'))

            # 2. Encontra o BLOCO do Título
            titulo_block = item.find('div', class_='md:line-clamp-3')
            if titulo_block:
                # Dentro dele, procura o <h2> e depois o <a>
                titulo_h2 = titulo_block.find('h2')
                if titulo_h2:
                    link_tag = titulo_h2.find('a', href=True)
                    if link_tag:
                        # O texto do título está dentro da tag <a>
                        titulo_texto = link_tag.get_text(strip=True)

            if titulo_texto and link_tag and categoria_tag:
                titulo = titulo_texto
                link = link_tag['href']
                categoria = categoria_tag.get_text(strip=True)
                
                # Garante que o link é absoluto
                if not link.startswith('http'):
                    link = f"https://www.infomoney.com.br{link}"
                
                noticia_doc = {
                    'titulo': titulo,
                    'link': link,
                    'categoria': categoria,
                    'fonte': 'InfoMoney',
                    'data_coleta': datetime.now(UTC)
                }
                noticias_list.append(noticia_doc)
            else:
                print(f"AVISO: Card pulado. 'titulo': {bool(titulo_texto)}, 'link': {bool(link_tag)}, 'categoria': {bool(categoria_tag)}")
        
        print(f"Scraping concluído. Extraídas {len(noticias_list)} notícias válidas.")
        return noticias_list

    except requests.RequestException as e:
        print(f"Erro ao buscar a página do InfoMoney: {e}")
        return []

def popular_banco(collection, noticias):
    """Insere as notícias no banco de dados, limpando dados antigos"""
    if not noticias:
        print("Nenhuma notícia para inserir")
        return
    
    try:
        print(f"Limpando coleção '{MONGO_COLLECTION_NAME}'...")
        delete_result = collection.delete_many({})
        print(f"{delete_result.deleted_count} documentos antigos removidos.")

        print(f"Inserindo {len(noticias)} novas notícias...")
        result = collection.insert_many(noticias)
        print(f"Dados inseridos com sucesso. {len(result.inserted_ids)} documentos adicionados.")
        
    except errors.PyMongoError as e:
        print(f"Erro ao inserir dados no MongoDB: {e}")

# --- Bloco de execução principal (Main) ---
if __name__ == "__main__":
    print("--- Iniciando Script de Scraper do Infomoney")
    collection = connect_to_mongo()

    if collection is not None:
        noticias = scrape_infomoney()
        popular_banco(collection, noticias)
    else:
        print("Falha ao popular o banco. Conexão com o MongoDB não estabelecida")
        sys.exit(1)
    
    print("--- Script de Scraper Finalizado ---")