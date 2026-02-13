# Dossier de configuration

Ce dossier contient les fichiers de configuration pour le projet d'analyse de données e-commerce Olist.

## Fichiers de configuration

### 1. `config.yaml` - Configuration principale
Fichier de configuration minimal pour les scripts de traitement de données :
```yaml
paths:
  raw_data: "data/raw"
  processed_data: "data/processed"
  reports: "reports"
```

### 2. `data_config.yaml` - Configuration des données
Configuration détaillée pour le traitement des données :
- **Chemins des données** : raw, processed, external
- **Fichiers du dataset** : Liste des 9 CSV attendus
- **Paramètres de traitement** : Encodage, délimiteur, format de date
- **Paramètres d'analyse** : Seed aléatoire, taille des tests, niveau de confiance

### 3. `project_config.yaml` - Configuration du projet
Paramètres génériques pour le projet (nom, version, etc.)

### 4. `settings.yaml` - Paramètres supplémentaires
Configuration pour les fonctionnalités spécifiques (API, notifications, etc.)

## Utilisation des configurations

Les fichiers YAML sont chargés par les scripts via la bibliothèque `pyyaml` :
```python
import yaml

with open('config/data_config.yaml', 'r') as f:
    config = yaml.safe_load(f)
```

## Modification

Pour adapter le projet à votre environnement :

1. **Modifier les chemins** : Si vous avez un arborescence différente
2. **Changer les paramètres** : Seed aléatoire, format de date, etc.
3. **Ajouter des options** : Pour de nouvelles fonctionnalités

## Structure des données

Les chemins configurés dans `data_config.yaml` sont utilisés pour :
- Lire les données brutes depuis `data/raw/`
- Écrire les données traitées dans `data/processed/`
- Générer les rapports dans `reports/`
