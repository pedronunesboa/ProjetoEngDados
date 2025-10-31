import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, regexp_replace,
    explode, date_format, year, weekofyear, avg, sum, expr
)

def main():
    """
    Função principal para o job de transformação Spark (ELT).
    Lê dados da Camada Bronze (S3), transforma (Silver/Gold) e salva de volta no S3
    em formato Delta Lake.
    """
    print("---Iniciando job Spark de transformação marketpulse---")

    # -------------- 1. Ler credenciais AWS das variáveis de ambiente --------------
    # O Airflow (DockerOperator) vai injetar essas variáveis
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1') # default se não for passado

    if not aws_access_key_id or not aws_secret_access_key:
        print("ERRO: Credenciais AWS não encontradas nas variáveis de ambiente.")
        # Em um cenário real, poderiamos lançar um tratamento de erro aqui (except)
        raise ValueError ("Credenciais AWS ausentes")
        
    
    # -------------- 2. Configurar a sessão spark --------------
    print("Configurando a sessão spark para acesso ao S3...")

    spark = (
        SparkSession.builder
        .appName("MarketPulseTransform")
        # Configurações para acesso ao S3 via s3a (Hadoop AWS connector)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        # Adiciona os pacotes necessários do Hadoop AWS
        # A versão do hadoop-aws tem que ser compatível com a base Spark
        # Configurações para habilitar o Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    print("Sessão Spark configurada")

    # -------------- 3. Definindo os caminhos do S3 --------------
    s3_bucket_name = "marketpulse-bronze-layer-pedroboa-20251006"

    bronze_stocks_path = f"s3a://{s3_bucket_name}/stock_data/*/*.json" # Lê todas as subpastas
    bronze_news_path = f"s3a://{s3_bucket_name}/news_data/*.json"

    # Definindo Silver/Gold
    silver_stocks_path = f"s3a://{s3_bucket_name}/silver/cotacoes/"
    silver_news_path = f"s3a://{s3_bucket_name}/silver/noticias/"

    
    gold_agg_news_path = f"s3a://{s3_bucket_name}/gold/agg_noticias_por_dia"
    gold_agg_stocks_path = f"s3a://{s3_bucket_name}/gold/agg_acoes_semanal/"

    # -------------- 4. Lendo os dados da camada bronze --------------
    print(f"Lendo dados de Ações da camada Bronze: {bronze_stocks_path}")
    try:
        # A API Alpha Vantage retorna um JSON com um nó principal "Time Series (Daily)"
        # Usamos multiline=True para ler o JSON como um único registro
        # Depois precisaremos "explodir" o mapa de datas
        df_stocks_raw = spark.read.option("multiline", "true").json(bronze_stocks_path)

    except Exception as e:
        print(f"Erro ao ler dados de ações do S3: {e}")
        raise # Falha explícita
    
    print(f"Lendo dados de notícias da camada bronze: {bronze_news_path}")
    try:
        # O JSON de notícias é uma lista de objetos, o spark lê isso diretamente
        df_news_raw = spark.read.option("multiline", "true").json(bronze_news_path)
    except Exception as e:
        print(f"Erro ao ler dados de notícias do S3: {e}")
        raise

    # ===================================================================
    # --- 5. Transformação BRONZE -> SILVER ---
    # ===================================================================
    print("Iniciando transformação Bronze > Silver...")

    # --- 5.1. Processando notícias (silver)
    print("Processando notícias para camada Silver")
    df_news_silver = (
        df_news_raw
        .withColumn("noticias_id", col("_id.$oid")) # Desaninha o ObjectID do Mongo
        .withColumn("data_coleta_ts", to_timestamp(col("data_coleta.$date"))) # Converte a string de data
        .withColumn("data_particao", date_format(col("data_coleta_ts"), "yyyy-MM-dd"))
        .select(
            "noticias_id",
            col("titulo").alias("titulo_noticia"),
            col("link").alias("link_noticia"),
            col("categoria").alias("categoria_noticia"),
            col("fonte").alias("fonte_noticia"),
            col("data_coleta_ts").alias("data_coleta"),
            "data_particao"
        )
    )

    # Salva na camada Silver como Delta, particionando por nada
    (
        df_news_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("data_particao")
        .save(silver_news_path)
    )
    print(f"Camada Silver de notícias salva em {silver_news_path}")

    # --- 5.2 Processando ações (Silver) ---
    print("Processando Ações para camada Silver")

    # 1. Pegar o schema do Struct 'Time Series (Daily)'
    #    (O [0] pega o schema da primeira linha, assumindo que todos são iguais)
    time_series_schema = df_stocks_raw.schema["Time Series (Daily)"].dataType

    # 2. Pegar a lista de todos os campos de data (ex: "2025-06-11", "2025-06-12", ...)
    date_fields = time_series_schema.fieldNames()
    
    # 3. Construir a expressão 'stack'
    #    Formato: stack(N, 'data1', `Time Series (Daily)`.`data1`, 'data2', `Time Series (Daily)`.`data2`, ...)
    stack_expr_parts = []
    for date_str in date_fields:
        stack_expr_parts.append(f"'{date_str}'") # A data como string
        stack_expr_parts.append(f"`Time Series (Daily)`.`{date_str}`") # A coluna struct
            
    stack_expression = f"stack({len(date_fields)}, {', '.join(stack_expr_parts)}) as (data_str, daily_data)"
    
    print(f"Expressão Stack criada para {len(date_fields)} colunas de data.")

    # 4. Aplicar a expressão stack (unpivot)
    df_stocks_stacked = (
        df_stocks_raw
        .select(
            col("`Meta Data`.`2. Symbol`").alias("symbol"),
            expr(stack_expression)
        )
        .where(col("daily_data").isNotNull()) # Remove dias que não existem em um arquivo
    )
    

    # Limpando e tipando os dados
    df_stocks_silver = (
        df_stocks_stacked
        .withColumn("data_referencia", to_date(col("data_str")))
        # Desaninha o struct 'daily_data'
        .withColumn("open", regexp_replace(col("daily_data.`1. open`"), "[^0-9.]", "").cast("double"))
        .withColumn("high", regexp_replace(col("daily_data.`2. high`"), "[^0-9.]", "").cast("double"))
        .withColumn("low", regexp_replace(col("daily_data.`3. low`"), "[^0-9.]", "").cast("double"))
        .withColumn("close", regexp_replace(col("daily_data.`4. close`"), "[^0-9.]", "").cast("double"))
        .withColumn("volume", regexp_replace(col("daily_data.`5. volume`"), "[^0-9]", "").cast("long"))
        .withColumn("data_particao", date_format(col("data_referencia"), "yyyy-MM-dd"))
        .select("symbol", "data_referencia", "open", "high", "low", "close", "volume", "data_particao")
    )   

    # Salva na camada silver como delta, particionando por nada
    (
        df_stocks_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("data_particao")
        .save(silver_stocks_path)
    )
    print(f"Camada silver de ações salva em: {silver_stocks_path}")
    
    # ===================================================================
    # --- 6. Transformação SILVER -> GOLD (Agregações) ---
    # ===================================================================
    print("Iniciando transformação Silver -> Gold")

    # --- 6.1. Agregação de notícias por dia (Gold) ---
    df_gold_news_agg = (
        df_news_silver
        .groupBy("data_particao", "categoria_noticia")
        .count()
        .withColumnRenamed("count", "total_noticias")
    )

    (
        df_gold_news_agg.write
        .format("delta")
        .mode("overwrite")
        .save(gold_agg_news_path)
    )
    print(f"Camada Gold 'agg_noticias_por_dia' salva em: {gold_agg_news_path}")

    # --- 6.2. Agregação semanal de ações (Gold) ---
    df_gold_stocks_agg = (
        df_stocks_silver
        .withColumn("ano", year(col("data_referencia")))
        .withColumn("semana", weekofyear(col("data_referencia")))
        .groupBy("symbol", "ano", "semana")
        .agg(
            avg("close").alias("preco_medio_fechamento_semanal"),
            sum("volume").alias("volume_total_semanal")
        )
    )

    (
        df_gold_stocks_agg.write
        .format("delta")
        .mode("overwrite")
        .save(gold_agg_stocks_path)
    )
    print(f"Camada gold 'agg_acoes_semanal' salva em: {gold_agg_stocks_path}")

    print("--- Job Spark de transformação (ELT Completo) Concluído ---")
    spark.stop()


# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main()