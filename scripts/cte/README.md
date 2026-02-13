# Scripts de tables de bord (CTE)

Ce dossier contient les scripts Python pour les tables de bord et les analyses avancées e-commerce Olist.

## Scripts disponibles

### 1. `get_customer_payment_rank.py` - Rang des clients par paiement
**Fonctionnalité** : Classement des clients par montant total payé

**Principales tâches** :
- Calcul du montant total payé par client
- Rangement des clients
- Distribution des paiements
- Analyse des tendances

**Sorties** :
- Rapports CSV dans `reports/cte/`
- Visualisations dans les dashboards

### 2. `get_order_and_customer_payment_details.py` - Détails des paiements
**Fonctionnalité** : Réconciliation des commandes et des paiements

**Principales tâches** :
- Vérification de l'intégrité des paiements
- Détails des modes de paiement
- Anomalies financières
- Rapport de réconciliation

**Sorties** :
- Rapports CSV dans `reports/cte/`
- Analyses financières dans les dashboards

### 3. `get_customer_order_interval.py` - Intervalle entre les commandes
**Fonctionnalité** : Analyse de l'intervalle entre les commandes des clients

**Principales tâches** :
- Calcul de l'intervalle entre commandes
- Analyse de la fidélité
- Tendances temporelles
- Segmentation des clients

**Sorties** :
- Rapports CSV dans `reports/cte/`
- Visualisations dans les dashboards

## Utilisation

### Exécution individuelle
```bash
python scripts/cte/get_customer_payment_rank.py
python scripts/cte/get_order_and_customer_payment_details.py
python scripts/cte/get_customer_order_interval.py
```

### Exécution via le pipeline
```bash
python run_pipeline.py
```

## Prérequis

Pour utiliser ces scripts, vous avez besoin :
1. Des données brutes dans `data/raw/`
2. Les dépendances installées (`pip install -r requirements.txt`)
3. Les données traitées générées par les scripts de transformation et d'analyse

## Configuration

Les scripts utilisent la configuration de :
- `config/data_config.yaml` : Chemin des fichiers
- `config/config.yaml` : Paramètres de traitement

## Sorties

Tous les scripts génèrent :
- Fichiers CSV de résultats
- Visualisations interactives Plotly
- Données pour les dashboards

## Modification

Pour adapter les scripts :
1. Modifiez les paramètres de configuration
2. Ajoutez de nouveaux indicateurs
3. Ajustez les règles de calcul

## Notes

Les scripts sont exécutés automatiquement par le pipeline
et les résultats sont utilisés dans les dashboards Streamlit
et les rapports statiques générés par `generate_dashboards.py`.
