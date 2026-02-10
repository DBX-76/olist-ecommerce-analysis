"""
Script pour enrichir les données clients avec les informations géographiques
à partir de la table de référence géographique.
Ce script implémente la logique du modèle dbt customers_with_geolocation en Python.
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

def enrich_customers_with_geolocation():
    """Enrichit les données clients avec les informations géographiques"""
    
    print("="*80)
    print("ENRICHISSEMENT DES CLIENTS AVEC GÉOLOCALISATION")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données clientes standardisées
    print("Chargement des données clientes standardisées...")
    customers_std_file = os.path.join(PROCESSED_DATA_PATH, 'customers_standardized.csv')
    df_customers_std = pd.read_csv(customers_std_file)
    
    print(f"[OK] Données clientes standardisées chargées: {df_customers_std.shape[0]:,} lignes x {df_customers_std.shape[1]} colonnes")
    
    # Charger la table de référence géographique
    print("Chargement de la table de référence géographique...")
    zip_ref_file = os.path.join(PROCESSED_DATA_PATH, 'zip_code_reference.csv')
    df_zip_ref = pd.read_csv(zip_ref_file)
    
    print(f"[OK] Table de référence chargée: {df_zip_ref.shape[0]:,} lignes x {df_zip_ref.shape[1]} colonnes")
    
    # Afficher les structures pour comprendre les colonnes
    print(f"\nColonnes clients standardisés: {list(df_customers_std.columns)}")
    print(f"Colonnes référence: {list(df_zip_ref.columns)}")
    
    # Fusionner les données clientes avec les informations géographiques
    df_customers_geo = df_customers_std.merge(
        df_zip_ref[['zip_code_prefix', 'state', 
                   'avg_latitude', 'avg_longitude', 
                   'coordinate_samples', 'data_quality',
                   'lat_spread_km', 'lon_spread_km']],
        left_on=['customer_zip_code_prefix', 'customer_state'],
        right_on=['zip_code_prefix', 'state'],
        how='left'
    )
    
    # Renommer les colonnes pour correspondre à la structure attendue
    df_customers_geo = df_customers_geo.rename(columns={
        'coordinate_samples': 'geo_coordinate_samples',
        'data_quality': 'geo_data_quality'
    })
    
    # Sélectionner les colonnes pertinentes pour le résultat final
    result_columns = [
        'customer_id',
        'customer_unique_id',
        'customer_zip_code_prefix',
        'original_city_name',
        'standardized_city_name',
        'customer_state',
        'city_name_standardized',
        'avg_latitude',
        'avg_longitude',
        'geo_coordinate_samples',
        'geo_data_quality',
        'lat_spread_km',
        'lon_spread_km'
    ]
    
    df_customers_enriched = df_customers_geo[result_columns].copy()
    
    print(f"\nDonnées clientes enrichies: {df_customers_enriched.shape[0]:,} lignes x {df_customers_enriched.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(df_customers_enriched.head())
    
    # Afficher des statistiques sur l'enrichissement
    total_customers = len(df_customers_enriched)
    customers_with_geo = df_customers_enriched['avg_latitude'].notna().sum()
    percentage_with_geo = round((customers_with_geo / total_customers) * 100, 2)
    
    print(f"\nStatistiques d'enrichissement:")
    print(f"- Total clients: {total_customers:,}")
    print(f"- Clients avec géolocalisation: {customers_with_geo:,} ({percentage_with_geo}%)")
    
    # Afficher la répartition de la qualité des données géographiques
    if 'geo_data_quality' in df_customers_enriched.columns:
        print(f"\nRépartition de la qualité géographique:")
        print(df_customers_enriched['geo_data_quality'].value_counts())
    
    # Sauvegarder les données clientes enrichies
    output_path = os.path.join(PROCESSED_DATA_PATH, 'customers_with_geolocation.csv')
    df_customers_enriched.to_csv(output_path, index=False)
    print(f"\n[OK] Données clientes enrichies sauvegardées: {output_path}")
    
    # Générer un rapport de validation
    validation_report = {
        'total_customers': total_customers,
        'customers_with_geolocation': int(customers_with_geo),
        'geolocation_rate_percent': float(percentage_with_geo),
        'avg_coordinates_per_zip': df_customers_enriched['geo_coordinate_samples'].mean() if 'geo_coordinate_samples' in df_customers_enriched.columns else 0,
        'high_quality_locations': int(df_customers_enriched[df_customers_enriched['geo_data_quality'] == 'High'].shape[0]) if 'geo_data_quality' in df_customers_enriched.columns else 0
    }
    
    # Sauvegarder le rapport de validation
    report_path = os.path.join(REPORTS_PATH, 'customers_geolocation_enrichment_report.csv')
    report_df = pd.DataFrame([validation_report])
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Rapport d'enrichissement sauvegardé: {report_path}")
    
    print(f"\nProcessus d'enrichissement terminé avec succès!")
    return df_customers_enriched

if __name__ == "__main__":
    enrich_customers_with_geolocation()