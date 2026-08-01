"""
Tests unitaires pour les transformations PySpark du processeur de tickets.
Vérifie la logique d'enrichissement et d'agrégation sans nécessiter de cluster Kafka/Redpanda.
"""

import os
import shutil
import pytest

# Ignore proprement les tests de cette suite si PySpark n'est pas installé dans l'environnement local
pyspark = pytest.importorskip("pyspark", reason="PySpark n'est pas installé dans l'environnement Python local")

# Vérification de l'environnement Java requis par PySpark
if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
    pytest.skip("Java (JRE/JDK) non présent sur l'hôte local. PySpark fonctionne dans le conteneur Docker (OpenJDK 17).", allow_module_level=True)

try:
    from pyspark.sql import SparkSession  # type: ignore
except ImportError:
    SparkSession = None  # type: ignore

from src.processor.spark_processor import enrich_ticket_data, aggregate_ticket_metrics, TICKET_SCHEMA

@pytest.fixture(scope="module")
def spark_test_session() -> SparkSession:
    """
    Fixture pytest fournissant une SparkSession locale minimale pour les tests unitaires.
    """
    spark = SparkSession.builder \
        .appName("UnitTestPySpark") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_enrich_ticket_data(spark_test_session: SparkSession) -> None:
    """
    Vérifie l'assignation de l'équipe de support et le calcul des SLA en fonction du type et de la priorité.
    """
    test_data = [
        ("TCK-001", "CUST-101", "2026-08-01T12:00:00Z", "Panne SQL", "TECHNICAL", "URGENT"),
        ("TCK-002", "CUST-102", "2026-08-01T12:05:00Z", "Facture 2026", "BILLING", "LOW"),
        ("TCK-003", "CUST-103", "2026-08-01T12:10:00Z", "Bug UI", "BUG", "HIGH"),
    ]

    input_df = spark_test_session.createDataFrame(test_data, schema=TICKET_SCHEMA)
    enriched_df = enrich_ticket_data(input_df)

    rows = enriched_df.collect()
    row_dict = {r["ticket_id"]: r for r in rows}

    # Vérification TCK-001 (TECHNICAL + URGENT -> TechSupport_Tier2, SLA 2h)
    assert row_dict["TCK-001"]["support_team"] == "TechSupport_Tier2"
    assert row_dict["TCK-001"]["sla_hours"] == 2

    # Vérification TCK-002 (BILLING + LOW -> Finance_Ops, SLA 48h)
    assert row_dict["TCK-002"]["support_team"] == "Finance_Ops"
    assert row_dict["TCK-002"]["sla_hours"] == 48

    # Vérification TCK-003 (BUG + HIGH -> DevOps_Squad, SLA 6h)
    assert row_dict["TCK-003"]["support_team"] == "DevOps_Squad"
    assert row_dict["TCK-003"]["sla_hours"] == 6

def test_aggregate_ticket_metrics(spark_test_session: SparkSession) -> None:
    """
    Vérifie l'agrégation du nombre total de tickets par type de demande.
    """
    test_data = [
        ("TCK-001", "CUST-101", "2026-08-01T12:00:00Z", "Erreur SQL", "TECHNICAL", "HIGH"),
        ("TCK-002", "CUST-102", "2026-08-01T12:05:00Z", "Autre SQL", "TECHNICAL", "HIGH"),
        ("TCK-003", "CUST-103", "2026-08-01T12:10:00Z", "Devis", "BILLING", "LOW"),
    ]

    input_df = spark_test_session.createDataFrame(test_data, schema=TICKET_SCHEMA)
    enriched_df = enrich_ticket_data(input_df)
    metrics_df = aggregate_ticket_metrics(enriched_df)

    results = metrics_df.collect()
    tech_count = [r["total_tickets"] for r in results if r["request_type"] == "TECHNICAL"][0]

    assert tech_count == 2, "La comptabilisation des tickets TECHNICAL doit être égale à 2"
