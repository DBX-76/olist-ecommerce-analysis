# Guide d'Analyse de la Qualité des Données

## Vue d'ensemble

Ce guide explique comment identifier et résoudre les problèmes de qualité des données dans votre dataset e-commerce brésilien. Le projet inclut maintenant une analyse complète avec standardisation géographique, détection d'anomalies financières et analyse approfondie des produits et avis.

## Rapports Générés

L'analyse a généré des rapports détaillés dans le dossier [`../reports/`](../reports/):

1. **missing_values_report.csv** - Analyse détaillée des valeurs manquantes
2. **data_types_report.csv** - Information sur les types de données
3. **duplicates_report.csv** - Analyse des lignes dupliquées
4. **outliers_report.csv** - Détection des valeurs aberrantes
5. **data_quality_summary.csv** - Tableau de bord de la qualité globale
6. **cleaning_recommendations.csv** - Recommandations d'action
7. **Anomaly Detection Reports** - Analyses détaillées dans [`../reports/anomaly_detection/`](../reports/anomaly_detection/)

---

## Étapes d'Analyse Complètes

### 1. Analyse de Qualité des Données (Initiale)

#### Problèmes HAUTE Priorité:

**Dataset: order_reviews**
- `review_comment_title`: 87,656 valeurs manquantes (88.34%)
- `review_comment_message`: 58,247 valeurs manquantes (58.70%)

**Action Recommandée:**
```python
# Ces colonnes ont été supprimées dans le nettoyage initial
df_reviews = df_reviews.drop(['review_comment_title', 'review_comment_message'], axis=1)
```

#### Problèmes FAIBLE Priorité:

**Dataset: orders**
- `order_delivered_customer_date`: 2,965 valeurs manquantes (2.98%)
- `order_delivered_carrier_date`: 1,783 valeurs manquantes (1.79%)
- `order_approved_at`: 160 valeurs manquantes (0.16%)

**Action Recommandée:**
```python
# Analyser le statut des commandes avec dates manquantes
missing_dates = df_orders[df_orders['order_delivered_customer_date'].isna()]
print(missing_dates['order_status'].value_counts())

# Pour les commandes non livrées, les dates manquantes sont normales
# Ne pas imputer ces valeurs
```

**Dataset: products**
- 8 colonnes avec 610 valeurs manquantes (1.85%)

**Action Recommandée:**
```python
# Imputer avec la médiane pour les colonnes numériques
numeric_cols = ['product_name_lenght', 'product_description_lenght',
                'product_photos_qty', 'product_weight_g',
                'product_length_cm', 'product_height_cm', 'product_width_cm']
for col in numeric_cols:
    df_products[col] = df_products[col].fillna(df_products[col].median())

# Pour la catégorie, utiliser "unknown"
df_products['product_category_name'] = df_products['product_category_name'].fillna('unknown')
```

---

### 2. Données Dupliquées (MOYENNE)

**Dataset: geolocation**
- 261,831 lignes dupliquées (26.18%)

**Action Recommandée:**
```python
# Doublons supprimés dans le nettoyage initial
df_geolocation = df_geolocation.drop_duplicates()
```

---

### 3. Standardisation Géographique (Étape Avancée)

#### Création de la table de référence géographique
- **`zip_code_reference.csv`** : Table canonique avec noms de villes standardisés et coordonnées géographiques
- **Statistiques géographiques** : Moyennes, écarts-types, étendues géographiques
- **Indicateurs de qualité** : Basés sur le nombre d'échantillons par code postal

#### Standardisation des clients et vendeurs
- **`customers_standardized.csv`** : Clients avec villes standardisées
- **`sellers_standardized.csv`** : Vendeurs avec villes standardisées
- **`customers_with_geolocation.csv`** : Clients enrichis avec coordonnées
- **`sellers_with_geolocation.csv`** : Vendeurs enrichis avec coordonnées

---

### 4. Analyse Financière et Réconciliation

#### Anomalies Détectées:
- **Montants non réconciliés** : 1,153 commandes (1.16%) - dues aux taxes ICMS brésiliennes
- **Commandes livrées SANS paiement** : 1 cas critique à documenter
- **Paiements à 0 $** : 9 cas - vouchers gratuits légitimes
- **Installments = 0** : 2 cas - erreurs techniques corrigées

#### Actions Prises:
```python
# Correction des installments = 0 pour cartes de crédit
mask_cc_zero = (order_payments['payment_type'] == 'credit_card') & (order_payments['payment_installments'] == 0)
order_payments.loc[mask_cc_zero, 'payment_installments'] = 1

# Remplacement des types 'not_defined' par 'voucher'
order_payments['payment_type'] = order_payments['payment_type'].replace('not_defined', 'voucher')
```

---

### 5. Analyse Approfondie des Anomalies

#### Produits:
- **Dimensions manquantes** : 2 produits physiques (normal pour produits digitaux)
- **Densité implausible** : 11 produits - unités corrigées (mm/cm, kg/g)
- **Produits sans photos** : 610 produits - comportement normal

#### Avis Clients:
- **Avis avant achat** : 74 cas critiques - timezone ou données corrompues
- **Avis silencieux** : 56,518 cas - comportement normal brésilien
- **Réponses vendeur** : 682 cas avec réponses tardives (>30j)

---

## Scripts Disponibles

### Scripts Principaux (`scripts/core/`)
- **`create_zip_code_reference.py`** : Création de la table de référence géographique
- **`standardize_customers.py`** : Standardisation des clients
- **`enrich_customers_with_geolocation.py`** : Enrichissement géo des clients
- **`detect_seller_anomalies.py`** : Détection anomalies vendeurs
- **`standardize_sellers.py`** : Standardisation des vendeurs
- **`enrich_sellers_with_geolocation.py`** : Enrichissement géo des vendeurs
- **`detect_clean_financial_anomalies.py`** : Détection et nettoyage anomalies financières
- **`analyze_financial_discrepancies.py`** : Analyse détaillée écarts financiers
- **`advanced_financial_cleaning.py`** : Nettoyage avancé basé sur analyse
- **`analyze_clean_products_reviews.py`** : Analyse anomalies produits/avis

### Scripts d'Analyse (`scripts/analysis/`)
- **`analyze_data_quality.py`** : Analyse de qualité des données
- **`clean_data.py`** : Nettoyage des données

### Scripts Utilitaires (`scripts/utils/`)
- **`organize_reports.py`** : Organisation des rapports
- **`test_profiling.py`** : Test de profilage des données

---

## Prochaines Étapes

1. ✅ **Analyser les rapports** - Comprendre les problèmes identifiés
2. ✅ **Prioriser les actions** - Commencer par les problèmes CRITIQUES
3. ✅ **Créer des scripts de nettoyage** - Automatiser le processus
4. ✅ **Valider les données nettoyées** - Vérifier la qualité
5. ✅ **Documenter les transformations** - Garder une trace
6. ✅ **Analyse avancée** - Procéder à l'analyse exploratoire
7. ✅ **Standardisation géographique** - Créer référentiel canonique
8. ✅ **Réconciliation financière** - Analyser écarts commande/paiement
9. ✅ **Analyse approfondie** - Produits et avis clients

---

## Résumé des Problèmes Traités

| Type d'Analyse | Problème | Action Prise | Statut |
|----------------|----------|--------------|--------|
| Valeurs manquantes | order_reviews (88.34%) | Colonnes supprimées | ✅ Résolu |
| Doublons | geolocation (26.18%) | Suppression doublons | ✅ Résolu |
| Anomalies géographiques | Noms de villes incohérents | Standardisation | ✅ Résolu |
| Anomalies financières | Écarts commande/paiement | Réconciliation | ✅ Résolu |
| Anomalies produits | Unités erronées | Correction kg/g, mm/cm | ✅ Résolu |
| Anomalies avis | Avis avant achat | Documentation | ✅ Résolu |

---

## Contact

Pour plus d'informations, consultez:
- [`../README.md`](../README.md) - Vue d'ensemble du projet
- [`../notebooks/README.md`](../notebooks/README.md) - Guide des notebooks
- [`../scripts/README.md`](../scripts/README.md) - Documentation des scripts