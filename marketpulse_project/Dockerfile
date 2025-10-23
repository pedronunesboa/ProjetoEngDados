# 1. Imagem Base: Começamos com uma imagem oficial do Python 3.9
FROM python:3.12.3

# 2. Define o diretório de trabalho dentro do container
WORKDIR /app

# 3. Copia o arquivo de dependências primeiro (para otimizar o cache)
COPY requirements.txt .

# 4. Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia o nosso script de extração para dentro do container
COPY extract.py .

# 6. Define o comando que será executado quando o container iniciar
CMD ["python", "extract.py"]