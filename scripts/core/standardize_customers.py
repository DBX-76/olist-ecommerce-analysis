"""
Script pour standardiser les données clients en utilisant la table de référence géographique.
Ce script implémente la logique du modèle dbt customers_standardized en Python.
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

def standardize_customers():
    """Standardise les données clients en utilisant la table de référence géographique"""
    
    print("="*80)
    print("STANDARDISATION DES DONNÉES CLIENTS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données clients
    print("Chargement des données clients...")
    customers_file = os.path.join(RAW_DATA_PATH, 'olist_customers_dataset.csv')
    df_customers = pd.read_csv(customers_file)
    
    print(f"[OK] Données clients chargées: {df_customers.shape[0]:,} lignes x {df_customers.shape[1]} colonnes")
    
    # Charger la table de référence géographique
    print("Chargement de la table de référence géographique...")
    zip_ref_file = os.path.join(PROCESSED_DATA_PATH, 'zip_code_reference.csv')
    df_zip_ref = pd.read_csv(zip_ref_file)
    
    print(f"[OK] Table de référence chargée: {df_zip_ref.shape[0]:,} lignes x {df_zip_ref.shape[1]} colonnes")
    
    # Afficher les structures pour comprendre les colonnes
    print(f"\nColonnes clients: {list(df_customers.columns)}")
    print(f"Colonnes référence: {list(df_zip_ref.columns)}")
    
    # Renommer les colonnes si nécessaire pour correspondre à la structure attendue
    df_customers_renamed = df_customers.rename(columns={
        'customer_zip_code_prefix': 'customer_zip_code_prefix',
        'customer_city': 'customer_city',
        'customer_state': 'customer_state'
    })
    
    # Créer une copie pour la standardisation
    df_customers_std = df_customers_renamed.copy()
    
    # Ajouter le nom de ville canonique en joignant avec la table de référence
    df_customers_std = df_customers_std.merge(
        df_zip_ref[['zip_code_prefix', 'state', 'canonical_city_name']],
        left_on=['customer_zip_code_prefix', 'customer_state'],
        right_on=['zip_code_prefix', 'state'],
        how='left'
    )
    
    # Créer la colonne du nom de ville standardisé
    # Utiliser le nom canonique si disponible, sinon conserver l'original
    df_customers_std['standardized_city_name'] = df_customers_std['canonical_city_name'].fillna(df_customers_std['customer_city'])
    
    # Marquer les villes qui ont été standardisées
    df_customers_std['city_name_standardized'] = (
        (df_customers_std['canonical_city_name'].notna()) & 
        (df_customers_std['canonical_city_name'] != df_customers_std['customer_city'])
    ).astype(int)
    
    # Conserver les noms de villes originaux pour référence
    df_customers_std['original_city_name'] = df_customers_std['customer_city']
    
    # Sélectionner les colonnes pertinentes pour le résultat final
    result_columns = [
        'customer_id',
        'customer_unique_id', 
        'customer_zip_code_prefix',
        'original_city_name',
        'standardized_city_name',
        'customer_state',
        'city_name_standardized'
    ]
    
    df_customers_final = df_customers_std[result_columns].copy()
    
    print(f"\nDonnées clientes standardisées: {df_customers_final.shape[0]:,} lignes x {df_customers_final.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(df_customers_final.head())
    
    # Afficher des statistiques sur la standardisation
    total_customers = len(df_customers_final)
    standardized_customers = df_customers_final['city_name_standardized'].sum()
    percentage_standardized = round((standardized_customers / total_customers) * 100, 2)
    
    print(f"\nStatistiques de standardisation:")
    print(f"- Total clients: {total_customers:,}")
    print(f"- Clients avec ville standardisée: {standardized_customers:,} ({percentage_standardized}%)")
    
    # Sauvegarder les données clientes standardisées
    output_path = os.path.join(PROCESSED_DATA_PATH, 'customers_standardized.csv')
    df_customers_final.to_csv(output_path, index=False)
    print(f"\n[OK] Données clientes standardisées sauvegardées: {output_path}")
    
    # Générer un rapport de validation
    validation_report = {
        'total_customers': total_customers,
        'customers_with_standardized_cities': int(standardized_customers),
        'standardization_rate_percent': float(percentage_standardized),
        'cities_before_standardization': int(df_customers['customer_city'].nunique()),
        'cities_after_standardization': int(df_customers_final['standardized_city_name'].nunique())
    }
    
    # Sauvegarder le rapport de validation
    customers_report_dir = os.path.join(REPORTS_PATH, 'customers')
    os.makedirs(customers_report_dir, exist_ok=True)
    report_path = os.path.join(customers_report_dir, 'customers_standardization_report.csv')
    report_df = pd.DataFrame([validation_report])
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Rapport de standardisation sauvegardé: {report_path}")
    
    print(f"\nProcessus de standardisation terminé avec succès!")
    return df_customers_final

if __name__ == "__main__":
    standardize_customers()