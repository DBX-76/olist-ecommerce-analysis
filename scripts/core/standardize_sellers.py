"""
Script pour standardiser les données des vendeurs en se basant sur les anomalies détectées.
Ce script implémente la logique du modèle dbt sellers_standardized en Python.
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

def standardize_sellers():
    """Standardise les données des vendeurs en se basant sur les anomalies détectées"""
    
    print("="*80)
    print("STANDARDISATION DES DONNÉES VENDEURS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données des vendeurs avec anomalies
    print("Chargement des données des vendeurs avec anomalies...")
    sellers_anomalies_file = os.path.join(PROCESSED_DATA_PATH, 'sellers_location_anomalies.csv')
    df_sellers_anomalies = pd.read_csv(sellers_anomalies_file)
    
    print(f"[OK] Données des vendeurs avec anomalies chargées: {df_sellers_anomalies.shape[0]:,} lignes x {df_sellers_anomalies.shape[1]} colonnes")
    
    # Créer une copie pour la standardisation
    df_sellers_std = df_sellers_anomalies.copy()
    
    # Appliquer les règles de nettoyage en fonction des anomalies détectées
    def clean_city_name(row):
        original_city = row['seller_city']
        canonical_city = row['canonical_city_name']
        
        # Si la ville est numérique, utiliser le nom canonique
        if row['anomaly_numeric_city'] == 1:
            return canonical_city if pd.notna(canonical_city) else original_city
        
        # Si la ville contient des slashs, prendre la première partie
        if row['anomaly_contains_slashes'] == 1 and pd.notna(original_city):
            parts = str(original_city).split('/')
            return parts[0].strip()
        
        # Si la ville contient des virgules, prendre la première partie
        if row['anomaly_contains_commas'] == 1 and pd.notna(original_city):
            parts = str(original_city).split(',')
            return parts[0].strip()
        
        # Sinon, utiliser le nom canonique si disponible, sinon conserver l'original
        return canonical_city if pd.notna(canonical_city) else original_city
    
    # Appliquer la fonction de nettoyage
    df_sellers_std['standardized_city_name'] = df_sellers_std.apply(clean_city_name, axis=1)
    
    # Marquer les vendeurs qui ont été standardisés
    df_sellers_std['was_standardized'] = (
        (df_sellers_std['anomaly_numeric_city'] == 1) |
        (df_sellers_std['anomaly_contains_slashes'] == 1) |
        (df_sellers_std['anomaly_contains_commas'] == 1) |
        (df_sellers_std['has_canonical_mismatch'] == 1)
    ).astype(int)
    
    # Conserver les noms de villes originaux pour référence
    df_sellers_std['original_city_name'] = df_sellers_std['seller_city']
    
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
        'anomaly_extra_spaces',
        'has_canonical_mismatch'
    ]
    
    df_sellers_final = df_sellers_std[result_columns].copy()
    
    print(f"\nDonnées des vendeurs standardisées: {df_sellers_final.shape[0]:,} lignes x {df_sellers_final.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(df_sellers_final.head())
    
    # Afficher des statistiques sur la standardisation
    total_sellers = len(df_sellers_final)
    standardized_sellers = df_sellers_final['was_standardized'].sum()
    percentage_standardized = round((standardized_sellers / total_sellers) * 100, 2)
    
    print(f"\nStatistiques de standardisation:")
    print(f"- Total vendeurs: {total_sellers:,}")
    print(f"- Vendeurs avec ville standardisée: {standardized_sellers:,} ({percentage_standardized}%)")
    
    # Afficher la répartition des types d'anomalies traitées
    print(f"\nRépartition des anomalies détectées:")
    anomaly_columns = [
        'anomaly_numeric_city', 'anomaly_contains_slashes', 'anomaly_contains_commas',
        'anomaly_contains_brasil', 'anomaly_too_short', 'anomaly_extra_spaces', 'has_canonical_mismatch'
    ]
    
    for col in anomaly_columns:
        count = df_sellers_final[col].sum()
        pct = round((count / total_sellers) * 100, 2)
        print(f"- {col}: {count:,} ({pct}%)")
    
    # Sauvegarder les données des vendeurs standardisées
    output_path = os.path.join(PROCESSED_DATA_PATH, 'sellers_standardized.csv')
    df_sellers_final.to_csv(output_path, index=False)
    print(f"\n[OK] Données des vendeurs standardisées sauvegardées: {output_path}")
    
    # Générer un rapport de validation
    validation_report = {
        'total_sellers': total_sellers,
        'sellers_with_standardized_cities': int(standardized_sellers),
        'standardization_rate_percent': float(percentage_standardized),
        'cities_before_standardization': int(df_sellers_anomalies['seller_city'].nunique()),
        'cities_after_standardization': int(df_sellers_final['standardized_city_name'].nunique())
    }
    
    # Ajouter les statistiques d'anomalies
    for col in anomaly_columns:
        validation_report[f'{col}_count'] = int(df_sellers_final[col].sum())
        validation_report[f'{col}_percentage'] = float(round((df_sellers_final[col].sum() / total_sellers) * 100, 2))
    
    # Sauvegarder le rapport de validation
    sellers_report_dir = os.path.join(REPORTS_PATH, 'sellers')
    os.makedirs(sellers_report_dir, exist_ok=True)
    report_path = os.path.join(sellers_report_dir, 'sellers_standardization_report.txt')
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("RAPPORT DE STANDARDISATION DES DONNÉES VENDEURS\n")
        f.write("="*80 + "\n\n")
        for key, value in validation_report.items():
            # Formater les clés pour une meilleure lisibilité
            formatted_key = key.replace('_', ' ').capitalize()
            if isinstance(value, float):
                f.write(f"{formatted_key}: {value:.2f}\n")
            else:
                f.write(f"{formatted_key}: {value:,}\n")
    print(f"[OK] Rapport de standardisation sauvegardé: {report_path}")
    
    print(f"\nProcessus de standardisation terminé avec succès!")
    return df_sellers_final

if __name__ == "__main__":
    standardize_sellers()