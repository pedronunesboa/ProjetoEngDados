# Nome do Arquivo: marketpulse_transform/Dockerfile

# 1. Imagem Base com Spark e Python
# Usamos a imagem Bitnami que é popular e bem mantida.
# Escolha uma versão compatível com a que você usou no Databricks (ex: Spark 3.5)
FROM apache/spark:3.5.1-python3

# 2. Instalar bibliotecas Python adicionais
#    - boto3: Para interagir com AWS S3
#    - s3fs: Uma dependência comum para Spark acessar S3
#    - pyspark: (Geralmente já vem, mas garantimos a versão)
#    - delta-spark: (Opcional, se quisermos usar Delta Lake localmente no futuro)
# Mudando para o usuário root/0 para instalar pacotes e configurar permissões
USER 0          

# Instalar as bibliotecas python adicionais
RUN pip install --no-cache-dir boto3 s3fs delta-spark && \
    # Limpa o cache do pip
    rm -rf /root/.cache/pip

# 3. CRIAR DIRETÓRIOS IVY COM PERMISSÕES CORRETAS
# Criamos tanto /tmp/.ivy2 quanto /home/spark/.ivy2 por segurança
RUN mkdir -p /tmp/.ivy2 /home/spark/.ivy2 && \
    chmod -R 777 /tmp/.ivy2 && \
    chown -R 185:185 /home/spark/.ivy2 && \
    chmod -R 755 /home/spark/.ivy2

# 4. . Copiar nosso script de transformação para dentro da imagem
COPY transform.py /opt/spark/work-dir/transform.py

# 4. Voltar para o usuário não root (boa prática)
# A imagem base do spark usa o UID 185
USER 185

# 5. # Definir ponto de entrada com configurações Ivy corretas
# Usar javaOptions em vez de spark.ivy.user.dir
ENTRYPOINT [ \
    "/opt/spark/bin/spark-submit", \
    "--packages", \
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-spark_2.12:3.1.0", \
    "--conf", \
    "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2", \
    "/opt/spark/work-dir/transform.py" \
]

# CMD pode ser usado para passar argumentos, se necessário no futuro
# CMD []