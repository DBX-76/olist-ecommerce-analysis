# Module d'analyse e-commerce

Ce module fournit une analyse complète du dataset e-commerce Olist, axée sur les indicateurs clés de performance (KPIs) et les métriques business.

## 📊 Analyses disponibles

### 💰 Métriques de ventes
- Tendances quotidiennes, mensuelles et annuelles des revenus
- Analyse de croissance YoY (année sur année)
- Top 10 produits les plus performants
- Prévisions de revenus

### 👥 Métriques clients
- Analyse nouveaux vs clients fidèles
- Valeur moyenne du panier
- Suivi du taux de conversion
- Analyse RFM (Récence, Fréquence, Montant)

### 📊 Analyse de cohortes
- Retention des clients par mois d'acquisition
- Valeur vie client (LTV) par cohorte
- Suivi de performance basé sur les cohortes

## 🛠️ Configuration

### Prérequis
- Base de données PostgreSQL avec le dataset Olist chargé
- Python 3.7+

### Installation
```bash
pip install -r requirements.txt
```

## 🚀 Utilisation

### 1. Générer des dashboards statiques
```bash
python analytics/generate_dashboards.py
```
Crée des dashboards HTML interactifs dans le dossier `reports/` :
- `reports/sales_dashboard.html`
- `reports/customer_dashboard.html`
- `reports/cohort_dashboard.html`

### 2. Lancer les tests de performance
```bash
python analytics/performance_test.py
```
Analyse les performances des requêtes avant/après optimisation, créant une stratégie d'indexation.

### 3. Dashboard interactif Streamlit
```bash
streamlit run analytics/streamlit_dashboard.py
```
Lance un dashboard web interactif avec des données en temps réel depuis la base de données.

## 📋 Requêtes SQL optimisées

Le fichier `kpi_queries.sql` contient des requêtes optimisées pour tous les KPIs :

1. **Analyse des revenus** : Jour, mois, année avec comparaisons YoY
2. **Performance des produits** : Top 10 produits par revenu et nombre de commandes
3. **Segmentation des clients** : Nouveaux vs clients fidèles
4. **Analyse RFM** : Segmentation basée sur récence, fréquence, montant
5. **Analyse de cohortes** : Taux de rétention et LTV par cohorte

### Optimisation des performances
- Indexation stratégique sur les colonnes clés
- JOINs performants
- Utilisation de CTEs et fenêtres de fonctions
- Mesure et reporting des temps d'exécution

## 📊 Fonctionnalités des dashboards

### Sales Dashboard
- Tendances des revenus au fil du temps
- Performance mensuelle et annuelle
- Catégories de produits les plus performantes
- Analyse du taux de croissance

### Customer Dashboard
- Acquisition et rétention des clients
- Suivi de la valeur moyenne du panier
- Monitoring du taux de conversion
- Segmentation RFM des clients

### Cohort Dashboard
- Visualisation de la matrice de rétention
- LTV par cohorte d'acquisition
- Suivi de performance des cohortes

## 📈 Indicators clés de performance

### KPIs de ventes
- Revenu total (jour/mois/année)
- Croissance des revenus (MoM/YoY)
- Meilleurs produits
- Valeur moyenne des commandes

### KPIs clients
- Ratio nouveaux vs clients fidèles
- Coût d'acquisition client
- Valeur moyenne du panier
- Taux de conversion

### KPIs de cohortes
- Taux de rétention des clients
- Valeur vie client (LTV)
- Suivi de performance des cohortes
- Analyse du churn rate

## 🤝 Contribution

1. Créez des requêtes optimisées dans `kpi_queries.sql`
2. Ajoutez des fonctions de visualisation dans `generate_dashboards.py`
3. Testez les améliorations de performance avec `performance_test.py`
4. Mettez à jour le dashboard Streamlit

## 📄 Licence

Ce projet fait partie de l'analyse e-commerce Olist.