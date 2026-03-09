# --- Imagen base con Spark y Hadoop ---
FROM apache/spark:3.5.0

# --- Variables de entorno ---
ENV PYTHON_VERSION=3
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.607
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# --- Instalación de Python y utilidades ---
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        curl \
        wget && \
    rm -rf /var/lib/apt/lists/*

# --- Configura python y pip ---
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# --- Instalación de librerías de Python ---
RUN pip install --no-cache-dir pandas pyspark==3.5.0 boto3

# --- Descargar e instalar los JARs de S3A ---
RUN mkdir -p ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars

# --- JARs para Structured Streaming + Kafka (consumer_forecast.py) ---
ENV SPARK_VERSION=3.5.0
ENV KAFKA_CLIENTS_VERSION=3.4.1
ENV COMMONS_POOL2_VERSION=2.11.1
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar -P ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar -P ${SPARK_HOME}/jars

# --- Directorio de trabajo y copiar código ---
WORKDIR /app
# Copiamos solo el contenido de nuestra carpeta app local
COPY ./app /app

# --- Mantener contenedor activo ---
CMD ["tail", "-f", "/dev/null"]
