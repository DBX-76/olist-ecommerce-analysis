# Notebooks d'analyse

Ce dossier contient les notebooks Jupyter pour analyser les données e-commerce Olist.

## Listes des notebooks

### 1. `01_Exploratory_Data_Analysis.ipynb`
**Exploration des données** :
- Statistiques descriptives
- Visualisations des distributions
- Analyse des variables clés
- Exploration géographique

### 2. `02_Data_Quality_Analysis.ipynb`
**Qualité des données** :
- Vérification des valeurs manquantes
- Détection d'anomalies
- Analyse des duplicatas
- Génération de rapports de qualité

### 3. `03_Customer_Payment_Rank.ipynb`
**Paiements clients** :
- Analyse des modes de paiement
- Rangs des clients par montant payé
- Distribution des paiements
- Tendances temporelles

### 4. `04_Customer_Payment_Details.ipynb`
**Détails des paiements** :
- Réconciliation commandes-paiements
- Analyse des anomalies financières
- Détails des modalités de paiement
- Vérification des intégrités

### 5. `05_Customer_Order_Interval.ipynb`
**Intervalle entre commandes** :
- Analyse de la fidélité des clients
- Intervalle entre les commandes
- Tendances de récurrence
- Segmentation des clients

## Comment exécuter

### Avec Jupyter Notebook
```bash
# Installer Jupyter
pip install jupyter

# Lancer Jupyter
jupyter notebook

# Ouvrir et exécuter les notebooks
```

### Avec le pipeline
Les notebooks sont exécutés automatiquement par le pipeline :
```bash
python run_pipeline.py
```

## Prérequis

Pour exécuter les notebooks, vous avez besoin :
1. Des données brutes dans `data/raw/`
2. Des données traitées dans `data/processed/`
3. Les dépendances installées (`pip install -r requirements.txt`)

## Résultats

Chaque notebook génère :
- Visualisations interactives (Plotly, Seaborn)
- Rapports de qualité en HTML
- Fichiers CSV avec les résultats

## Notes

- Les notebooks sont exécutés automatiquement lors du workflow GitHub Actions
- Les outputs des notebooks sont commités pour la reproductibilité
- Les visualisations interractives nécessitent un serveur Jupyter
