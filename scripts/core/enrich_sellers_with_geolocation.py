"""
Script pour enrichir les données des vendeurs avec les informations géographiques
à partir de la table de référence géographique.
Ce script implémente la logique du modèle dbt sellers_with_geolocation en Python.
"""

import pandas as pd
import numpy as np
import os
import yaml
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

def enrich_sellers_with_geolocation():
    """Enrichit les données des vendeurs avec les informations géographiques"""
    
    print("="*80)
    print("ENRICHISSEMENT DES VENDEURS AVEC GÉOLOCALISATION")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données des vendeurs standardisées
    print("Chargement des données des vendeurs standardisées...")
    sellers_std_file = os.path.join(PROCESSED_DATA_PATH, 'sellers_standardized.csv')
    df_sellers_std = pd.read_csv(sellers_std_file)
    
    print(f"[OK] Données des vendeurs standardisées chargées: {df_sellers_std.shape[0]:,} lignes x {df_sellers_std.shape[1]} colonnes")
    
    # Charger la table de référence géographique
    print("Chargement de la table de référence géographique...")
    zip_ref_file = os.path.join(PROCESSED_DATA_PATH, 'zip_code_reference.csv')
    df_zip_ref = pd.read_csv(zip_ref_file)
    
    print(f"[OK] Table de référence chargée: {df_zip_ref.shape[0]:,} lignes x {df_zip_ref.shape[1]} colonnes")
    
    # Afficher les structures pour comprendre les colonnes
    print(f"\nColonnes vendeurs standardisés: {list(df_sellers_std.columns)}")
    print(f"Colonnes référence: {list(df_zip_ref.columns)}")
    
    # Fusionner les données des vendeurs avec les informations géographiques
    df_sellers_geo = df_sellers_std.merge(
        df_zip_ref[['zip_code_prefix', 'state', 
                   'avg_latitude', 'avg_longitude', 
                   'coordinate_samples', 'data_quality',
                   'lat_spread_km', 'lon_spread_km']],
        left_on=['seller_zip_code_prefix', 'seller_state'],
        right_on=['zip_code_prefix', 'state'],
        how='left'
    )
    
    # Renommer les colonnes pour correspondre à la structure attendue
    df_sellers_geo = df_sellers_geo.rename(columns={
        'coordinate_samples': 'geo_coordinate_samples',
        'data_quality': 'geo_data_quality'
    })
    
    # Sélectionner les colonnes pertinentes pour le résultat final
    result_columns = [
        'seller_id',
        'seller_zip_code_prefix',
        'original_city_name',
        'standardized_city_name',
        'seller_state',
        'was_standardized',
        'anomaly_numeric_city',
        'anomaly_contains_slashes',
        'anomaly_contains_commas',
        'anomaly_contains_brasil',
        'anomaly_too_short',
        'avg_latitude',
        'avg_longitude',
        'geo_coordinate_samples',
        'geo_data_quality',
        'lat_spread_km',
        'lon_spread_km'
    ]
    
    df_sellers_enriched = df_sellers_geo[result_columns].copy()
    
    print(f"\nDonnées des vendeurs enrichies: {df_sellers_enriched.shape[0]:,} lignes x {df_sellers_enriched.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(df_sellers_enriched.head())
    
    # Afficher des statistiques sur l'enrichissement
    total_sellers = len(df_sellers_enriched)
    sellers_with_geo = df_sellers_enriched['avg_latitude'].notna().sum()
    percentage_with_geo = round((sellers_with_geo / total_sellers) * 100, 2)
    
    print(f"\nStatistiques d'enrichissement:")
    print(f"- Total vendeurs: {total_sellers:,}")
    print(f"- Vendeurs avec géolocalisation: {sellers_with_geo:,} ({percentage_with_geo}%)")
    
    # Afficher la répartition de la qualité des données géographiques
    if 'geo_data_quality' in df_sellers_enriched.columns:
        print(f"\nRépartition de la qualité géographique:")
        print(df_sellers_enriched['geo_data_quality'].value_counts())
    
    # Sauvegarder les données des vendeurs enrichies
    output_path = os.path.join(PROCESSED_DATA_PATH, 'sellers_with_geolocation.csv')
    df_sellers_enriched.to_csv(output_path, index=False)
    print(f"\n[OK] Données des vendeurs enrichies sauvegardées: {output_path}")
    
    # Générer un rapport de validation
    validation_report = {
        'total_sellers': total_sellers,
        'sellers_with_geolocation': int(sellers_with_geo),
        'geolocation_rate_percent': float(percentage_with_geo),
        'avg_coordinates_per_zip': df_sellers_enriched['geo_coordinate_samples'].mean() if 'geo_coordinate_samples' in df_sellers_enriched.columns else 0,
        'high_quality_locations': int(df_sellers_enriched[df_sellers_enriched['geo_data_quality'] == 'High'].shape[0]) if 'geo_data_quality' in df_sellers_enriched.columns else 0
    }
    
    # Sauvegarder le rapport de validation
    sellers_report_dir = os.path.join(REPORTS_PATH, 'sellers')
    os.makedirs(sellers_report_dir, exist_ok=True)
    report_path = os.path.join(sellers_report_dir, 'sellers_geolocation_enrichment_report.csv')
    report_df = pd.DataFrame([validation_report])
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Rapport d'enrichissement sauvegardé: {report_path}")
    
    print(f"\nProcessus d'enrichissement terminé avec succès!")
    return df_sellers_enriched

if __name__ == "__main__":
    enrich_sellers_with_geolocation()