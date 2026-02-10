"""
Script pour créer la table de référence géographique optimisée (zip_code_reference)
à partir des données de géolocalisation.

Ce script implémente la logique du modèle dbt zip_code_reference en Python.
"""

import pandas as pd
import numpy as np
import os
import yaml
from scipy import stats
from collections import Counter
import warnings

warnings.filterwarnings('ignore')

def load_configuration():
    """Charge la configuration depuis le fichier YAML"""
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    RAW_DATA_PATH = config['paths']['raw_data']
    PROCESSED_DATA_PATH = config['paths']['processed_data']
    REPORTS_PATH = config['paths']['reports']
    
    return RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH

def get_canonical_city_name(city_series):
    """
    Retourne le nom canonique d'une ville (celui qui apparaît le plus fréquemment)
    """
    if len(city_series) == 0:
        return None
    
    # Compter les occurrences de chaque nom de ville
    city_counts = Counter(city_series)
    # Retourner le nom le plus fréquent
    return city_counts.most_common(1)[0][0]

def create_zip_code_reference():
    """Crée la table de référence géographique optimisée"""
    
    print("="*80)
    print("CRÉATION DE LA TABLE DE RÉFÉRENCE GÉOGRAPHIQUE (zip_code_reference)")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données de géolocalisation
    print("Chargement des données de géolocalisation...")
    geolocation_file = os.path.join(RAW_DATA_PATH, 'olist_geolocation_dataset.csv')
    df_geo = pd.read_csv(geolocation_file)
    
    print(f"[OK] Données chargées: {df_geo.shape[0]:,} lignes x {df_geo.shape[1]} colonnes")
    
    # Afficher les premières lignes pour comprendre la structure
    print("\nStructure des données de géolocalisation:")
    print(df_geo.head())
    print(f"\nColonnes disponibles: {list(df_geo.columns)}")
    
    # Regrouper par préfixe de code postal et état
    print("\nRegroupement des données par préfixe de code postal et état...")
    
    # Renommer les colonnes pour correspondre à la structure attendue
    df_geo_renamed = df_geo.rename(columns={
        'geolocation_zip_code_prefix': 'zip_code_prefix',
        'geolocation_lat': 'latitude',
        'geolocation_lng': 'longitude',
        'geolocation_city': 'city',
        'geolocation_state': 'state'
    })
    
    # S'assurer que les colonnes nécessaires existent
    required_cols = ['zip_code_prefix', 'state', 'city', 'latitude', 'longitude']
    for col in required_cols:
        if col not in df_geo_renamed.columns:
            print(f"ATTENTION: Colonne '{col}' non trouvée dans les données")
            if col == 'zip_code_prefix':
                # Essayer différentes variantes de nommage
                for alt_col in ['geolocation_zip_code_prefix', 'zip_code_prefix', 'customer_zip_code_prefix']:
                    if alt_col in df_geo.columns:
                        df_geo_renamed = df_geo.rename(columns={alt_col: 'zip_code_prefix'})
                        break
            elif col == 'latitude':
                for alt_col in ['geolocation_lat', 'lat', 'latitude']:
                    if alt_col in df_geo.columns:
                        df_geo_renamed = df_geo.rename(columns={alt_col: 'latitude'})
                        break
            elif col == 'longitude':
                for alt_col in ['geolocation_lng', 'lng', 'longitude', 'geolocation_long']:
                    if alt_col in df_geo.columns:
                        df_geo_renamed = df_geo.rename(columns={alt_col: 'longitude'})
                        break
            elif col == 'city':
                for alt_col in ['geolocation_city', 'city', 'customer_city']:
                    if alt_col in df_geo.columns:
                        df_geo_renamed = df_geo.rename(columns={alt_col: 'city'})
                        break
            elif col == 'state':
                for alt_col in ['geolocation_state', 'state', 'customer_state']:
                    if alt_col in df_geo.columns:
                        df_geo_renamed = df_geo.rename(columns={alt_col: 'state'})
                        break
    
    # Vérifier à nouveau les colonnes requises
    missing_cols = [col for col in required_cols if col not in df_geo_renamed.columns]
    if missing_cols:
        print(f"ERREUR: Colonnes manquantes: {missing_cols}")
        print(f"Colonnes disponibles: {list(df_geo_renamed.columns)}")
        return
    
    # Supprimer les lignes avec des valeurs manquantes critiques
    df_geo_clean = df_geo_renamed.dropna(subset=['zip_code_prefix', 'latitude', 'longitude'])
    print(f"Données nettoyées: {df_geo_clean.shape[0]:,} lignes restantes")
    
    # Fonction pour obtenir le nom canonique et le nombre de variations pour chaque groupe
    def get_city_stats(group):
        canonical_name = get_canonical_city_name(group['city'])
        variation_count = group['city'].nunique()
        return pd.Series({
            'canonical_city_name': canonical_name,
            'city_name_variations': variation_count,
            'avg_latitude': group['latitude'].mean(),
            'latitude_stddev': group['latitude'].std(),
            'min_latitude': group['latitude'].min(),
            'max_latitude': group['latitude'].max(),
            'avg_longitude': group['longitude'].mean(),
            'longitude_stddev': group['longitude'].std(),
            'min_longitude': group['longitude'].min(),
            'max_longitude': group['longitude'].max(),
            'coordinate_samples': len(group)
        })
    
    # Appliquer la fonction de regroupement
    grouped = df_geo_clean.groupby(['zip_code_prefix', 'state']).apply(get_city_stats).reset_index()
    
    # Calculer l'étendue géographique en km
    # 1 degré de latitude ≈ 111 km
    grouped['lat_spread_km'] = (grouped['max_latitude'] - grouped['min_latitude']) * 111
    # Pour la longitude, on ajuste selon la latitude moyenne
    grouped['lon_spread_km'] = (grouped['max_longitude'] - grouped['min_longitude']) * 111 * np.cos(np.radians(grouped['avg_latitude']))
    
    # Arrondir les valeurs pour plus de lisibilité
    grouped['lat_spread_km'] = grouped['lat_spread_km'].round(2)
    grouped['lon_spread_km'] = grouped['lon_spread_km'].round(2)
    
    # Déterminer la qualité des données
    conditions = [
        grouped['coordinate_samples'] >= 50,
        (grouped['coordinate_samples'] >= 10) & (grouped['coordinate_samples'] < 50),
        grouped['coordinate_samples'] < 10
    ]
    choices = ['High', 'Medium', 'Low']
    grouped['data_quality'] = np.select(conditions, choices)
    
    # Réorganiser les colonnes pour correspondre à la structure dbt
    final_columns = [
        'zip_code_prefix', 'state', 'canonical_city_name',
        'avg_latitude', 'avg_longitude',
        'min_latitude', 'max_latitude',
        'min_longitude', 'max_longitude',
        'latitude_stddev', 'longitude_stddev',
        'coordinate_samples', 'city_name_variations',
        'lat_spread_km', 'lon_spread_km',
        'data_quality'
    ]
    
    zip_code_ref = grouped[final_columns].copy()
    
    # Remplacer les NaN dans les colonnes de déviation standard par 0
    zip_code_ref['latitude_stddev'] = zip_code_ref['latitude_stddev'].fillna(0)
    zip_code_ref['longitude_stddev'] = zip_code_ref['longitude_stddev'].fillna(0)
    
    print(f"\nTable de référence créée: {zip_code_ref.shape[0]:,} lignes x {zip_code_ref.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(zip_code_ref.head())
    
    # Sauvegarder la table de référence
    output_path = os.path.join(PROCESSED_DATA_PATH, 'zip_code_reference.csv')
    zip_code_ref.to_csv(output_path, index=False)
    print(f"\n[OK] Table de référence sauvegardée: {output_path}")
    
    # Générer des statistiques
    print(f"\nStatistiques de la table de référence:")
    print(f"- Nombre total de préfixes ZIP: {zip_code_ref['zip_code_prefix'].nunique():,}")
    print(f"- Nombre total d'états couverts: {zip_code_ref['state'].nunique():,}")
    print(f"- Qualité des données:")
    print(zip_code_ref['data_quality'].value_counts())
    
    # Sauvegarder un rapport
    report_path = os.path.join(REPORTS_PATH, 'zip_code_reference_report.csv')
    report_data = {
        'metric': [
            'total_zip_prefixes',
            'total_states',
            'high_quality_records',
            'medium_quality_records', 
            'low_quality_records',
            'total_records'
        ],
        'value': [
            zip_code_ref['zip_code_prefix'].nunique(),
            zip_code_ref['state'].nunique(),
            len(zip_code_ref[zip_code_ref['data_quality'] == 'High']),
            len(zip_code_ref[zip_code_ref['data_quality'] == 'Medium']),
            len(zip_code_ref[zip_code_ref['data_quality'] == 'Low']),
            len(zip_code_ref)
        ]
    }
    report_df = pd.DataFrame(report_data)
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Rapport sauvegardé: {report_path}")
    
    print(f"\nProcessus terminé avec succès!")
    return zip_code_ref

if __name__ == "__main__":
    create_zip_code_reference()