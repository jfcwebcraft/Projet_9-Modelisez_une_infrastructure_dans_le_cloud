# Conteneurisation du processeur PySpark Structured Streaming
FROM python:3.11-slim

# Installation de Java Runtime (default-jre-headless) nécessaire pour PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/default-java"
WORKDIR /app

# Installation des dépendances Python (PySpark)
COPY requirements.txt .
RUN pip install --no-cache-dir pyspark==3.5.0

# Copie du code source
COPY src/processor/ ./src/processor/

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS="redpanda:9092"
ENV KAFKA_TOPIC="client_tickets"
ENV OUTPUT_PATH="/app/data/output/tickets"

CMD ["python", "src/processor/spark_processor.py"]
