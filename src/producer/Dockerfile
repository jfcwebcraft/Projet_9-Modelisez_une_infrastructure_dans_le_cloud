# Conteneurisation du producteur de tickets Redpanda Python
FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python

# Copie du code source
COPY src/producer/ ./src/producer/

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS="redpanda:9092"
ENV KAFKA_TOPIC="client_tickets"

CMD ["python", "src/producer/ticket_producer.py"]
