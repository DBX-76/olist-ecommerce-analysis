# Workflow GitHub Actions

Ce dossier contient le workflow GitHub Actions pour exécuter le pipeline d'analyse de données automatiquement.

## `pipeline.yml` - Workflow complet

Workflow qui s'exécute sur GitHub chaque fois que :
- Un commit est poussé sur la branche `main`
- Une pull request est ouverte sur la branche `main`

### Étapes du workflow

1. **Checkout** : Récupère le code du repository
2. **Setup Python** : Installe Python 3.10
3. **Install dependencies** : Installe les packages nécessaires
4. **Install Kaggle** : Installe l'API Kaggle
5. **Configure Kaggle** : Configure les credentials Kaggle
6. **Download dataset** : Télécharge les données brutes depuis Kaggle
7. **Create directories** : Crée les dossiers pour les données traitées et les rapports
8. **Run pipeline** : Exécute le pipeline complet (`run_pipeline.py`)
9. **Execute notebooks** : Exécute tous les notebooks Jupyter
10. **Commit notebooks** : Enregistre les notebooks avec leurs sorties
11. **Upload artifacts** : Enregistre les rapports et les données traitées

### Secrets requis

Pour que le workflow fonctionne, vous devez configurer les secrets GitHub suivants :

#### Kaggle
- `KAGGLE_USERNAME` : Votre nom d'utilisateur Kaggle
- `KAGGLE_KEY` : Votre clé API Kaggle (disponible sur votre profil Kaggle)

#### Base de données
- `DB_HOST` : Hôte de la base de données PostgreSQL
- `DB_PORT` : Port de la base de données (généralement 5432)
- `DB_NAME` : Nom de la base de données
- `DB_USER` : Nom d'utilisateur PostgreSQL
- `DB_PASSWORD` : Mot de passe PostgreSQL

## Utilisation

Le workflow est exécuté automatiquement, mais vous pouvez aussi le déclencher manuellement :

1. Allez dans l'onglet **Actions** de votre repository
2. Sélectionnez **Full Pipeline** dans la liste des workflows
3. Cliquez sur **Run workflow**
4. Choisissez la branche (généralement `main`)
5. Cliquez sur **Run workflow**

## Résultats

Les outputs du pipeline sont disponibles :
- Dans l'onglet **Artifacts** de l'exécution du workflow
- Les notebooks exécutés sont commités automatiquement
- Les rapports et les données traitées sont téléchargables

## Erreurs courantes

### "Kaggle API authentication failed"
- Vérifiez que les secrets Kaggle sont correctement configurés
- Vérifiez que votre API key est valide

### "Dataset not found"
- Vérifiez que le dataset Kaggle existe toujours
- Vérifiez le nom du dataset dans le workflow

## Modification

Pour adapter le workflow :
1. Changer la version de Python dans le step "Setup Python"
2. Modifier les dépendances dans `requirements.txt`
3. Ajouter ou supprimer des notebooks à exécuter
