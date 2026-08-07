# Étude de Compatibilité et Modélisation d'Architecture Cloud Hybride

**Projet :** Modernisation du système d'information et ingestion de flux IoT temps réel  
**Entreprise :** InduTechData  
**Rédacteur :** Jean-François Chaussoy  
**Date :** Août 2026  

---

## 1. Contexte et Enjeux Techniques

InduTechData gère aujourd'hui une infrastructure *on-premise* vieillissante qui arrive à saturation. L'installation de nouveaux capteurs industriels génère une augmentation constante de **50 Go de données par mois**.

L'infrastructure actuelle se compose de :
* **Un cluster SQL Server (40 To)** hébergeant les données critiques des applications métiers (ERP et CRM).
* **Une baie de stockage SAN (10 To)** dédiée aux fichiers utilisateurs, journaux systèmes et données brutes de capteurs.
* **Un serveur Active Directory (AD)** assurant l'authentification et la gestion centralisée des droits.

L'objectif de cette étude est d'étendre le système d'information vers l'écosystème AWS sans casser l'existant. L'infrastructure hybride proposée permet d'absorber l'ingestion temps réel des données IoT tout en conservant les bases de données SQL Server et l'annuaire Active Directory dans le datacenter d'InduTechData.

---

## 2. Choix d'Architecture et Services Sélectionnés

Pour répondre aux contraintes de volumétrie et de débit sans surdimensionner le matériel local, nous avons sélectionné quatre briques principales dans le cloud. *(Note : le schéma visuel global est disponible dans le Livrable 1).*

### A. Streaming Temps Réel : Cluster Redpanda
Plutôt qu'un déploiement Kafka traditionnel nécessitant la gestion d'un cluster ZooKeeper/KRaft et une JVM gourmande en mémoire, nous avons retenu **Redpanda**.
* Déployé sur des instances EC2 avec stockage local NVMe, Redpanda garantit une latence minime lors de l'ingestion des métriques transmises par les passerelles IoT locales.
* Sa compatibilité native avec l'API Kafka permet d'utiliser l'écosystème d'outils existant (notamment PySpark pour le traitement de flux) sans modifier la couche de développement.

### B. Stockage Objet Long Terme : Amazon S3
La baie SAN locale de 10 To ne pouvant pas absorber indéfiniment l'apport des 50 Go mensuels, le stockage d'objets Amazon S3 est utilisé comme niveau d'archivage principal.
* Redpanda s'appuie sur son mécanisme de *Tiered Storage* pour décharger automatiquement les données anciennes du stockage NVMe vers un compartiment S3.
* Les fichiers logs et les données brutes sont chiffrés au repos via AWS KMS (chiffrement AES-256).

### C. Entrepôt de Données Analytique : Amazon Redshift
Pour permettre aux équipes métiers d'exécuter des requêtes analytiques complexes sans impacter la production du cluster SQL Server local, nous intégrons **Amazon Redshift**.
* Les données agrégées par les pipelines de traitement (PySpark) sont déversées dans Redshift pour alimenter les outils décisionnels.

### D. Fédération d'Identités : AWS Managed Microsoft AD & AD Connector
Pour éviter de dupliquer les comptes utilisateurs et conserver un contrôle d'accès unique :
* Un **AWS AD Connector** est mis en place pour relier l'Active Directory *on-premise* à l'environnement AWS.
* Les collaborateurs conservent leurs identifiants habituels (SSO) pour accéder aux ressources cloud (consoles d'administration, requêtes Redshift, compartiments S3).

---

## 3. Compatibilité et Intégration avec le SI On-Premise

### Sécurité des communications inter-sites
Toutes les liaisons entre le datacenter InduTechData et la région AWS Europe (Paris) transitent par un **tunnel VPN IPsec redondant** (ou AWS Direct Connect selon l'évolution du trafic). Les flux Redpanda et les API de synchronisation utilisent exclusivement le protocole SSL/TLS (ports 9092 et 443).

### Synchronisation des données métiers (SQL Server vers Cloud)
Pour alimenter l'entrepôt Redshift à partir des tables SQL Server (ERP/CRM) sans verrouiller les bases de production, nous préconisons la mise en place d'un mécanisme de **Change Data Capture (CDC)** via **AWS Database Migration Service (DMS)**. Seules les modifications récentes sont répliquées au fil de l'eau.

### Automatisation des flux de données
L'ingestion et les transformations ne nécessitent aucune intervention manuelle. Les passerelles IoT locales envoient leurs métriques aux topics Redpanda, PySpark consomme et enrichit les données en continu, puis déverse les résultats dans S3 et Redshift.

---

## 4. Estimation Financière et Détaillée des Coûts

Pour éviter toute mauvaise surprise budgétaire, l'estimation a été calculée sur le tarificateur officiel *AWS Pricing Calculator* (Région Europe Paris - `eu-west-3`).

### A. Coûts Récurrents Mensuels (Environ 1 910 € / mois)

| Composant | Service / Ressource | Hypothèse de Calcul & Dimensionnement | Coût Estimé |
| :--- | :--- | :--- | :---: |
| **Streaming** | Cluster Redpanda | 3 instances EC2 `i3en.xlarge` (32 Go RAM, NVMe local) en haute disponibilité | ~850 € / mois |
| **Data Warehouse** | Amazon Redshift | Mode Serverless / nœuds RA3 pour requêtes analytiques et d'ingestion | ~450 € / mois |
| **Stockage** | Amazon S3 | ~10 To initiaux + 50 Go/mois (S3 Standard + transition S3 Glacier après 90 jours) | ~240 € / mois |
| **Identité** | AWS Managed AD | Annuaire géré Standard + AWS AD Connector | ~150 € / mois |
| **Réplication** | AWS DMS | 1 instance `dms.t3.medium` dédiée au flux CDC SQL Server | ~120 € / mois |
| **Réseau** | AWS Site-to-Site VPN | Tunnels IPsec redondants + frais de transfert de données | ~100 € / mois |
| **TOTAL RÉCURRENT** | | | **~1 910 € / mois** |

### B. Coûts d'Intégration Initiaux (Environ 3 500 € - Prestation unique)
Le budget d'implémentation initiale se décompose comme suit :
* **5 jours d'ingénierie DevOps / Cloud (TJM moyen 600 €/jour = 3 000 €) :**
  - Montage et recettes des tunnels VPN IPsec.
  - Configuration de la relation d'approbation Active Directory et d'AWS AD Connector.
  - Initialisation de la réplication CDC SQL Server vers Redshift.
* **Frais de migration initiale de données (~500 €) :**
  - Transfert initial du volume d'historique de 10 To du SAN local vers Amazon S3.

---

## 5. Points de Vigilance Opérationnels et Recommandations

1. **Capacité de la ligne réseau :** L'envoi continu de 50 Go par mois représente un flux moyen modéré, mais les pics de trafic IoT imposent de maintenir une bande passante montante garantie sur le lien internet du datacenter.
2. **Surveillance des coûts de sortie (Egress) :** Si les transferts vers le cloud sont peu coûteux, la ré-extraction massive de données depuis S3 vers le réseau local peut générer des frais de bande passante. Il est recommandé de conserver les traitements lourds directement dans l'environnement AWS.
3. **Pilotage budgétaire :** Définition d'alertes prédictives dans **AWS Budgets** pour notifier l'équipe d'ingénierie dès le franchissement de 80 % de l'enveloppe mensuelle (1 500 €).
