# Projet d'Analyse de Données E-commerce Brésilien Olist

[![Full Pipeline](https://github.com/DBX-76/olist-ecommerce-analysis/actions/workflows/pipeline.yml/badge.svg)](https://github.com/DBX-76/olist-ecommerce-analysis/actions/workflows/pipeline.yml)

## 📊 Ce que fait le projet

Analyse complète des données de l'e-commerce brésilien Olist :
- Nettoyage et standardisation des données
- Détection d'anomalies (géographiques, financières, produits)
- Enrichissement géographique
- Analyse des ventes et des clients
- Visualisations interactives

## 🚀 Comment ça marche ?

### Étape 1 : Installer les dépendances
```bash
pip install -r requirements.txt
```

### Étape 2 : Obtenir les données
Téléchargez les fichiers CSV du jeu de données e-commerce brésilien et placez-les dans le dossier `data/raw/`. Voir [data/README.md](data/README.md) pour plus d'informations.

### Étape 3 : Lancer le pipeline (recommandé)
Pour exécuter TOUS les traitements automatiquement :
```bash
python run_pipeline.py
```

### Étape 4 : Explorer les résultats
- **Notebooks d'analyse** : Dossier `notebooks/` (5 notebooks Jupyter avec visualisations)
- **Rapports générés** : Dossier `reports/` (fichiers HTML et CSV)
- **Dashboard interactif** :
  ```bash
  streamlit run analytics/streamlit_dashboard.py
  ```

## 📁 Structure du projet

```
Projet/
├── data/               # Données brutes et traitées
├── notebooks/          # Notebooks d'analyse (5 fichiers)
├── scripts/            # Traitements de données
├── analytics/          # Analyse des KPIs et dashboards
├── reports/            # Résultats générés
├── docs/               # Documentation
├── config/             # Configuration
└── run_pipeline.py     # Pipeline complet
```

## 📋 Ce qui est inclus

### Analyses
1. **Exploration des données** : Statistiques descriptives
2. **Qualité des données** : Vérification des anomalies
3. **Paiements clients** : Analyse des modes de paiement
4. **Détails des paiements** : Réconciliation commandes-paiements
5. **Intervalle entre commandes** : Analyse de la fidélité

### Outils
- **Streamlit Dashboard** : Visualisations interactives en temps réel
- **Tests de performance** : Mesure de la vitesse des requêtes
- **Génération de rapports** : Dashboards statiques avec Plotly

## 🔍 Pour aller plus loin

- [Guide d'analyse de la qualité](docs/Data_Quality_Analysis_Guide.md)
- [Schéma de la base de données](docs/sql/schema.md)
- [README des analyses](analytics/README.md)

## 📄 Licence

Données fournies par Olist - à des fins de recherche et d'éducation.