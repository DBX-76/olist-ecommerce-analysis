# Scripts de base de données

Ce dossier contient les scripts Python pour la gestion de la base de données PostgreSQL pour le projet Olist.

## Scripts disponibles

### 1. `init_db.py` - Initialisation de la base de données
**Fonctionnalité** : Crée le schéma de la base de données PostgreSQL

**Principales tâches** :
- Création des tables et des relations
- Définition des indexes
- Configuration des contraintes
- Initialisation de la base de données

**Sorties** :
- Schéma de base de données créé dans PostgreSQL

### 2. `load_data.py` - Chargement des données
**Fonctionnalité** : Charge les données traitées dans la base de données PostgreSQL

**Principales tâches** :
- Lecture des fichiers CSV des données traitées
- Insertion des données dans les tables correspondantes
- Vérification de l'intégrité des données
- Gestion des erreurs

**Sorties** :
- Données chargées dans la base de données
- Logs d'insertion

## Utilisation

### Configuration
Les scripts utilisent les variables d'environnement pour la connexion :
- `DB_HOST` : Hôte de la base de données
- `DB_PORT` : Port (généralement 5432)
- `DB_NAME` : Nom de la base de données
- `DB_USER` : Nom d'utilisateur
- `DB_PASSWORD` : Mot de passe

### Exécution individuelle
```bash
# Initialisation de la base de données
python scripts/db/init_db.py

# Chargement des données
python scripts/db/load_data.py
```

### Exécution via le pipeline
```bash
python run_pipeline.py
```

## Prérequis

Pour utiliser ces scripts, vous avez besoin :
1. PostgreSQL installé et fonctionnel
2. Les variables d'environnement configurées
3. Les données traitées dans `data/processed/`
4. Les dépendances installées (`pip install -r requirements.txt`)

## Configuration

Les scripts utilisent la configuration de :
- `config/data_config.yaml` : Chemin des fichiers
- Variables d'environnement : Connexion à la base de données

## Modification

Pour adapter les scripts :
1. Modifiez le schéma dans `init_db.py`
2. Ajustez les chemins de fichiers dans `data_config.yaml`
3. Modifiez les mappings table-fichier dans `load_data.py`

## Notes

Les scripts sont exécutés automatiquement par le pipeline
et les données sont utilisées pour les analyses et les dashboards.

### Erreurs courantes

- **Erreur de connexion** : Vérifiez les variables d'environnement
- **Fichier introuvable** : Vérifiez que les données traitées sont présentes
- **Erreur d'insertion** : Vérifiez le format des données
