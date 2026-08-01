"""
Tests unitaires pour le producteur de tickets clients.
Utilise pytest pour valider le format, la structure et le typage des tickets générés.
"""

import pytest
from typing import Dict, Any
from src.producer.ticket_producer import generate_random_ticket, REQUEST_TYPES, PRIORITIES

def test_generate_random_ticket_structure() -> None:
    """
    Vérifie que la fonction generate_random_ticket retourne un dictionnaire
    contenant toutes les clés obligatoires et des formats valides.
    """
    ticket: Dict[str, Any] = generate_random_ticket()

    # Vérification de la présence des clés
    required_keys = {"ticket_id", "customer_id", "created_at", "request", "request_type", "priority"}
    assert required_keys.issubset(ticket.keys()), "Des clés obligatoires sont manquantes dans le ticket"

    # Vérification des préfixes et valeurs
    assert ticket["ticket_id"].startswith("TCK-"), "L'ID du ticket doit commencer par TCK-"
    assert ticket["customer_id"].startswith("CUST-"), "L'ID du client doit commencer par CUST-"
    assert ticket["request_type"] in REQUEST_TYPES, f"Type de demande invalide : {ticket['request_type']}"
    assert ticket["priority"] in PRIORITIES, f"Priorité invalide : {ticket['priority']}"
    assert isinstance(ticket["request"], str) and len(ticket["request"]) > 0, "Le message de demande doit être une chaîne non vide"

def test_multiple_tickets_uniqueness() -> None:
    """
    Vérifie que la génération successive de tickets produit des ID uniques.
    """
    tickets = [generate_random_ticket() for _ in range(50)]
    ticket_ids = [t["ticket_id"] for t in tickets]
    
    assert len(set(ticket_ids)) == 50, "Les ID des tickets doivent être uniques"
