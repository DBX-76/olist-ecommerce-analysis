# Scripts d'analyse

Ce dossier contient les scripts Python pour l'analyse et la qualité des données e-commerce Olist.

## Scripts disponibles

### 1. `analyze_data_quality.py` - Analyse de la qualité des données
**Fonctionnalité** : Vérifie et analyse la qualité des données brutes et traitées

**Principales tâches** :
- Vérification des valeurs manquantes
- Détection de valeurs abérantes
- Analyse des duplicates
- Génération de rapports de qualité
- Calcul des indicateurs de qualité (completude, unicité, cohérence)

**Sorties** :
- Rapports CSV dans `reports/data_quality/`
- Rapports HTML dans `reports/eda/`

### 2. `clean_data.py` - Nettoyage des données
**Fonctionnalité** : Nettoie et standardise les données brutes

**Principales tâches** :
- Gestion des valeurs manquantes
- Suppression des duplicates
- Standardisation des formats
- Correction des anomalies

**Sorties** :
- Données nettoyées dans `data/processed/`
- Rapports de nettoyage dans `reports/cleaning/`

### 3. `data_profiling.py` - Profilage des données
**Fonctionnalité** : Génère un profil détaillé des données

**Principales tâches** :
- Statistiques descriptives
- Distributions des variables
- Corrélations
- Valeurs manquantes

**Sorties** :
- Rapports HTML dans `reports/eda/`

## Utilisation

### Exécution individuelle
```bash
# Analyse de qualité
python scripts/analysis/analyze_data_quality.py

# Nettoyage
python scripts/analysis/clean_data.py

# Profilage
python scripts/analysis/data_profiling.py
```

### Exécution via le pipeline
```bash
python run_pipeline.py
```

## Prérequis

Pour utiliser ces scripts, vous avez besoin :
1. Des données brutes dans `data/raw/`
2. Les dépendances installées (`pip install -r requirements.txt`)
3. Les données traitées générées par les scripts de transformation

## Configuration

Les scripts utilisent la configuration de :
- `config/data_config.yaml` : Chemin des fichiers
- `config/config.yaml` : Paramètres de traitement

## Sorties

Tous les scripts génèrent :
- Fichiers CSV de résultats
- Rapports HTML pour la visualisation
- Logs d'exécution

## Modification

Pour adapter les scripts :
1. Modifiez les paramètres de configuration
2. Ajoutez de nouveaux indicateurs de qualité
3. Ajustez les règles de nettoyage

## Notes

Les scripts sont exécutés automatiquement par le pipeline
et les résultats sont inclus dans les artifacts GitHub Actions.
