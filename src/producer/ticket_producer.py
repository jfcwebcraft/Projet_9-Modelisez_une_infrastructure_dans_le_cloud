"""
Producteur de tickets clients pour Redpanda (compatible API Apache Kafka).
Génère des flux continus de tickets synthétiques pour la simulation du POC InduTech.
"""

import json
import time
import random
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None  # type: ignore
    KafkaError = None  # type: ignore

# Configuration du logger en français
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("TicketProducer")

# Types de demandes et priorités supportées
REQUEST_TYPES: List[str] = ["TECHNICAL", "BILLING", "BUG", "FEATURE_REQUEST", "GENERAL"]
PRIORITIES: List[str] = ["LOW", "MEDIUM", "HIGH", "URGENT"]

SAMPLE_REQUEST_MESSAGES: Dict[str, List[str]] = {
    "TECHNICAL": [
        "Impossible de se connecter au serveur SQL local.",
        "Erreur de timeout lors de la transmission des métriques IoT.",
        "Problème d'allocation mémoire sur l'agent de collecte."
    ],
    "BILLING": [
        "Demande d'explication sur la facture du mois dernier.",
        "Mise à jour des coordonnées bancaires du compte client.",
        "Demande de devis pour augmentation de capacité de stockage."
    ],
    "BUG": [
        "L'export PDF du rapport d'analyse plante sur le tableau de bord.",
        "Le graphique de latence réseau affiche des valeurs négatives.",
        "Erreur 500 lors du clic sur le bouton de réinitialisation."
    ],
    "FEATURE_REQUEST": [
        "Ajouter un filtre par plage horaire sur la vue des capteurs.",
        "Export automatique des tickets vers le format CSV.",
        "Prise en charge du protocole MQTT 5.0."
    ],
    "GENERAL": [
        "Question sur la disponibilité du support pendant les jours fériés.",
        "Demande de documentation technique sur l'API v2.",
        "Demande d'accompagnement pour la formation des utilisateurs."
    ]
}

def generate_random_ticket() -> Dict[str, Any]:
    """
    Génère un dictionnaire représentant un ticket client complet avec typage strict.
    """
    req_type: str = random.choice(REQUEST_TYPES)
    req_msg: str = random.choice(SAMPLE_REQUEST_MESSAGES[req_type])
    
    ticket_payload: Dict[str, Any] = {
        "ticket_id": f"TCK-{uuid.uuid4().hex[:8].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": req_msg,
        "request_type": req_type,
        "priority": random.choice(PRIORITIES)
    }
    return ticket_payload

def create_kafka_producer(bootstrap_servers: str) -> Optional[KafkaProducer]:
    """
    Initialise et retourne une instance du producteur Kafka connecté à Redpanda.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda val: json.dumps(val).encode('utf-8'),
            key_serializer=lambda key: key.encode('utf-8') if key else None,
            retries=5,
            acks='all'
        )
        logger.info(f"Connexion réussie au cluster Redpanda sur {bootstrap_servers}")
        return producer
    except Exception as err:
        logger.error(f"Échec de la connexion au cluster Redpanda : {err}")
        return None

def start_producing(
    bootstrap_servers: str = "localhost:9092",
    topic_name: str = "client_tickets",
    interval_seconds: float = 1.0,
    max_messages: Optional[int] = None
) -> None:
    """
    Boucle principale d'émission de tickets synthétiques vers Redpanda.
    """
    producer = create_kafka_producer(bootstrap_servers)
    if not producer:
        logger.warning("Producteur non initialisé. Passage en mode simulation locale.")
        return

    logger.info(f"Début de la production de tickets sur le topic '{topic_name}'...")
    count: int = 0

    try:
        while True:
            ticket: Dict[str, Any] = generate_random_ticket()
            ticket_id: str = ticket["ticket_id"]
            
            future = producer.send(topic_name, key=ticket_id, value=ticket)
            record_metadata = future.get(timeout=10)
            
            count += 1
            logger.info(
                f"Ticket émis #{count} [{ticket_id}] -> Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
            )
            
            if max_messages and count >= max_messages:
                logger.info(f"Nombre maximal de messages atteint ({max_messages}). Arrêt de la production.")
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Interruption manuelle reçue. Fermeture du producteur.")
    except Exception as err:
        logger.error(f"Erreur inattendue durant l'émission de tickets : {err}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producteur arrêté proprement.")

if __name__ == "__main__":
    import os
    kafka_broker: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "client_tickets")
    start_producing(bootstrap_servers=kafka_broker, topic_name=topic, interval_seconds=1.5)
