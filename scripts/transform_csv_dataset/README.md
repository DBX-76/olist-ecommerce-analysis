# Scripts de transformation CSV

Ce dossier contient les scripts Python pour la transformation et la standardisation des données CSV brutes e-commerce Olist.

## Scripts disponibles

### 1. `create_zip_code_reference.py` - Référence des codes postaux
**Fonctionnalité** : Crée une table de référence géographique pour les codes postaux brésiliens

**Principales tâches** :
- Traitement des données de géolocalisation
- Détection des variants de noms de villes
- Calcul des coordonnées moyennes
- Génération de métriques de qualité des données

**Sorties** :
- `data/processed/zip_code_reference.csv`

### 2. `standardize_customers.py` - Standardisation des clients
**Fonctionnalité** : Standardise les données des clients

**Principales tâches** :
- Standardisation des noms de villes
- Vérification des codes postaux
- Gestion des anomalies

**Sorties** :
- `data/processed/customers_standardized.csv`

### 3. `enrich_customers_with_geolocation.py` - Enrichissement clients
**Fonctionnalité** : Enrichit les données des clients avec des informations géographiques

**Principales tâches** :
- Ajout des coordonnées géographiques
- Analyse de la dispersion
- Vérification de la cohérence des données

**Sorties** :
- `data/processed/customers_with_geolocation.csv`

### 4. `standardize_sellers.py` - Standardisation des vendeurs
**Fonctionnalité** : Standardise les données des vendeurs

**Principales tâches** :
- Standardisation des noms de villes
- Vérification des codes postaux
- Gestion des anomalies

**Sorties** :
- `data/processed/sellers_standardized.csv`

### 5. `enrich_sellers_with_geolocation.py` - Enrichissement vendeurs
**Fonctionnalité** : Enrichit les données des vendeurs avec des informations géographiques

**Principales tâches** :
- Ajout des coordonnées géographiques
- Analyse de la dispersion
- Vérification de la cohérence des données

**Sorties** :
- `data/processed/sellers_with_geolocation.csv`

### 6. `detect_seller_anomalies.py` - Détection d'anomalies vendeurs
**Fonctionnalité** : Détecte les anomalies dans les données des vendeurs

**Principales tâches** :
- Analyse de la qualité des données
- Détection de valeurs abérantes
- Rapport des anomalies

**Sorties** :
- Rapports dans `reports/anomaly_detection/`

### 7. `advanced_financial_cleaning.py` - Nettoyage financier avancé
**Fonctionnalité** : Nettoie les données financières des commandes et des paiements

**Principales tâches** :
- Réconciliation commandes-paiements
- Détection d'anomalies financières
- Traitement des exceptions
- Documentation des corrections

**Sorties** :
- `data/processed/financial_analysis/`
- Rapports dans `reports/cleaning/`

### 8. `analyze_clean_products_reviews.py` - Analyse produits et avis
**Fonctionnalité** : Analyse et nettoie les données des produits et des avis clients

**Principales tâches** :
- Vérification des dimensions des produits
- Analyse des anomalies d'avis
- Traitement des données de qualité

**Sorties** :
- `data/processed/product_review_analysis/`
- Rapports dans `reports/anomaly_detection/`

### 9. `detect_clean_financial_anomalies.py` - Détection anomalies financières
**Fonctionnalité** : Détecte les anomalies financières dans les données

**Principales tâches** :
- Analyse des montants des commandes
- Vérification des paiements
- Détection d'incohérences

**Sorties** :
- Rapports dans `reports/anomaly_detection/`

### 10. `generate_processed_data_schema.py` - Schéma des données traitées
**Fonctionnalité** : Génère le schéma des données traitées

**Principales tâches** :
- Création de la structure des données
- Définition des types de données
- Documentation des champs

**Sorties** :
- Schéma des données traitées

### 11. `generate_relational_schema.py` - Schéma relationnel
**Fonctionnalité** : Génère le schéma relationnel de la base de données

**Principales tâches** :
- Définition des tables et relations
- Configuration des contraintes
- Documentation du schéma

**Sorties** :
- Schéma relationnel

### 12. `optimize_schema.py` - Optimisation du schéma
**Fonctionnalité** : Optimise le schéma de la base de données

**Principales tâches** :
- Indexation des colonnes clés
- Optimisation des requêtes
- Configuration des performance

**Sorties** :
- Schéma optimisé

### 13. `merge_product_translations.py` - Fusion des traductions
**Fonctionnalité** : Fusionne les traductions des catégories de produits

**Principales tâches** :
- Fusion des tables de catégories et de traductions
- Gestion des valeurs manquantes
- Standardisation des données

**Sorties** :
- Données des produits avec traductions

## Utilisation

### Exécution individuelle
```bash
python scripts/transform_csv_dataset/create_zip_code_reference.py
python scripts/transform_csv_dataset/standardize_customers.py
python scripts/transform_csv_dataset/enrich_customers_with_geolocation.py
python scripts/transform_csv_dataset/standardize_sellers.py
python scripts/transform_csv_dataset/enrich_sellers_with_geolocation.py
python scripts/transform_csv_dataset/detect_seller_anomalies.py
python scripts/transform_csv_dataset/advanced_financial_cleaning.py
python scripts/transform_csv_dataset/analyze_clean_products_reviews.py
python scripts/transform_csv_dataset/detect_clean_financial_anomalies.py
python scripts/transform_csv_dataset/generate_processed_data_schema.py
python scripts/transform_csv_dataset/generate_relational_schema.py
python scripts/transform_csv_dataset/optimize_schema.py
python scripts/transform_csv_dataset/merge_product_translations.py
```

### Exécution via le pipeline
```bash
python run_pipeline.py
```

## Prérequis

Pour utiliser ces scripts, vous avez besoin :
1. Des données brutes dans `data/raw/`
2. Les dépendances installées (`pip install -r requirements.txt`)

## Configuration

Les scripts utilisent la configuration de :
- `config/data_config.yaml` : Chemin des fichiers
- `config/config.yaml` : Paramètres de traitement

## Sorties

Tous les scripts génèrent :
- Fichiers CSV de données traitées
- Rapports de qualité
- Logs d'exécution

## Modification

Pour adapter les scripts :
1. Modifiez les paramètres de configuration
2. Ajoutez de nouvelles règles de transformation
3. Ajustez les seuils de détection d'anomalies

## Notes

Les scripts sont exécutés automatiquement par le pipeline
et les données traitées sont utilisées pour les analyses
et les dashboards.
