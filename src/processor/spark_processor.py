"""
Processeur PySpark Structured Streaming pour l'analyse en temps réel des tickets Redpanda.
Enrichit les tickets avec l'équipe support dédiée, calcule le SLA et exporte les données.
"""

import os
import logging
from typing import Optional, Any

# Configuration des logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, from_json, expr, when, current_timestamp, window, count
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType
    )

    # Schéma JSON strict d'un ticket client
    TICKET_SCHEMA = StructType([
        StructField("ticket_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("request", StringType(), False),
        StructField("request_type", StringType(), False),
        StructField("priority", StringType(), False)
    ])
except ImportError:
    # Fallback propre pour le linter lorsque PySpark n'est exécuté que dans le conteneur Docker
    class DummyType:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    SparkSession = DummyType  # type: ignore
    DataFrame = Any  # type: ignore
    TICKET_SCHEMA = None  # type: ignore

def create_spark_session(app_name: str = "RedpandaTicketProcessor") -> SparkSession:
    """
    Initialise et configure la SparkSession avec le package Spark-Kafka.
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"SparkSession initialisée avec succès : {app_name}")
        return spark
    except Exception as err:
        logger.error(f"Échec de l'initialisation de SparkSession : {err}")
        raise

def enrich_ticket_data(raw_df: DataFrame) -> DataFrame:
    """
    Transforme et enrichit les données brutes des tickets :
    - Assignation de l'équipe de support en fonction de 'request_type'
    - Calcul du délai d'intervention SLA selon la 'priority'
    """
    enriched_df = raw_df \
        .withColumn("support_team",
            when(col("request_type") == "TECHNICAL", "TechSupport_Tier2")
            .when(col("request_type") == "BILLING", "Finance_Ops")
            .when(col("request_type") == "BUG", "DevOps_Squad")
            .when(col("request_type") == "FEATURE_REQUEST", "Product_Team")
            .otherwise("Helpdesk_Tier1")
        ) \
        .withColumn("sla_hours",
            when(col("priority") == "URGENT", 2)
            .when(col("priority") == "HIGH", 6)
            .when(col("priority") == "MEDIUM", 24)
            .otherwise(48)
        ) \
        .withColumn("processed_at", current_timestamp())

    return enriched_df

def aggregate_ticket_metrics(enriched_df: DataFrame) -> DataFrame:
    """
    Agrège le nombre de tickets reçus par type de demande et niveau de priorité.
    """
    metrics_df = enriched_df \
        .groupBy("request_type", "priority", "support_team") \
        .agg(count("ticket_id").alias("total_tickets"))
    
    return metrics_df

def start_streaming_pipeline(
    bootstrap_servers: str = "localhost:9092",
    topic_name: str = "client_tickets",
    output_path: str = "./data/output/tickets"
) -> None:
    """
    Lance le pipeline de streaming PySpark depuis Redpanda vers le stockage local JSON/Parquet.
    """
    logger.info("Démarrage du pipeline PySpark Structured Streaming...")
    spark = create_spark_session()

    try:
        # Lecture du stream Kafka / Redpanda
        raw_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()

        # Conversion du payload JSON
        parsed_stream_df = raw_stream_df \
            .selectExpr("CAST(value AS STRING) as json_payload") \
            .select(from_json(col("json_payload"), TICKET_SCHEMA).alias("data")) \
            .select("data.*")

        # Enrichissement
        enriched_stream_df = enrich_ticket_data(parsed_stream_df)

        # Output console pour débogage
        query_console = enriched_stream_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        # Output fichiers JSON pour persistance
        checkpoint_dir = os.path.join(output_path, "_checkpoint")
        json_output_dir = os.path.join(output_path, "json")

        query_json = enriched_stream_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", json_output_dir) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        logger.info(f"Pipeline streaming démarré. Données sauvegardées dans : {json_output_dir}")
        query_console.awaitTermination()
        query_json.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Interruption manuelle reçue. Fermeture du traitement PySpark.")
    except Exception as err:
        logger.error(f"Erreur d'exécution du pipeline PySpark : {err}")
    finally:
        spark.stop()
        logger.info("SparkSession fermée.")

if __name__ == "__main__":
    kafka_broker: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "client_tickets")
    out_dir: str = os.getenv("OUTPUT_PATH", "./data/output/tickets")
    
    start_streaming_pipeline(
        bootstrap_servers=kafka_broker,
        topic_name=topic,
        output_path=out_dir
    )
