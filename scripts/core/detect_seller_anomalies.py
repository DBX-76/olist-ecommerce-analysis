"""
Script pour détecter les anomalies dans les données des vendeurs.
Ce script implémente la logique du modèle dbt sellers_location_anomalies en Python.
"""

import pandas as pd
import numpy as np
import os
import yaml
import re
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

def detect_seller_anomalies():
    """Détecte les anomalies dans les données des vendeurs"""
    
    print("="*80)
    print("DÉTECTION DES ANOMALIES POUR LES VENDEURS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Charger les données des vendeurs
    print("Chargement des données des vendeurs...")
    sellers_file = os.path.join(RAW_DATA_PATH, 'olist_sellers_dataset.csv')
    df_sellers = pd.read_csv(sellers_file)
    
    print(f"[OK] Données des vendeurs chargées: {df_sellers.shape[0]:,} lignes x {df_sellers.shape[1]} colonnes")
    
    # Charger la table de référence géographique
    print("Chargement de la table de référence géographique...")
    zip_ref_file = os.path.join(PROCESSED_DATA_PATH, 'zip_code_reference.csv')
    df_zip_ref = pd.read_csv(zip_ref_file)
    
    print(f"[OK] Table de référence chargée: {df_zip_ref.shape[0]:,} lignes x {df_zip_ref.shape[1]} colonnes")
    
    # Afficher les structures pour comprendre les colonnes
    print(f"\nColonnes vendeurs: {list(df_sellers.columns)}")
    print(f"Colonnes référence: {list(df_zip_ref.columns)}")
    
    # Renommer les colonnes si nécessaire pour correspondre à la structure attendue
    df_sellers_renamed = df_sellers.rename(columns={
        'seller_zip_code_prefix': 'seller_zip_code_prefix',
        'seller_city': 'seller_city',
        'seller_state': 'seller_state'
    })
    
    # Créer une copie pour ajouter les indicateurs d'anomalie
    df_sellers_anomalies = df_sellers_renamed.copy()
    
    # Détecter différentes formes d'anomalies dans les noms de villes
    # 1. Villes avec seulement des chiffres
    df_sellers_anomalies['anomaly_numeric_city'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and str(x).strip().isdigit() else 0
    )
    
    # 2. Villes contenant des slashs
    df_sellers_anomalies['anomaly_contains_slashes'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and ('/' in str(x) or '\\' in str(x)) else 0
    )
    
    # 3. Villes contenant des virgules
    df_sellers_anomalies['anomaly_contains_commas'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and ',' in str(x) else 0
    )
    
    # 4. Villes contenant 'brasil' (insensible à la casse)
    df_sellers_anomalies['anomaly_contains_brasil'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and 'brasil' in str(x).lower() else 0
    )
    
    # 5. Villes trop courtes (moins de 3 caractères)
    df_sellers_anomalies['anomaly_too_short'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and len(str(x).strip()) < 3 else 0
    )
    
    # 6. Villes avec des espaces multiples ou des espaces inappropriés
    df_sellers_anomalies['anomaly_extra_spaces'] = df_sellers_anomalies['seller_city'].apply(
        lambda x: 1 if pd.notna(x) and ('  ' in str(x) or ' / ' in str(x)) else 0
    )
    
    # Joindre avec la table de référence pour comparer avec les noms canoniques
    df_sellers_anomalies = df_sellers_anomalies.merge(
        df_zip_ref[['zip_code_prefix', 'state', 'canonical_city_name']],
        left_on=['seller_zip_code_prefix', 'seller_state'],
        right_on=['zip_code_prefix', 'state'],
        how='left'
    )
    
    # Détecter les écarts par rapport au nom canonique
    df_sellers_anomalies['has_canonical_mismatch'] = (
        (df_sellers_anomalies['canonical_city_name'].notna()) & 
        (df_sellers_anomalies['canonical_city_name'] != df_sellers_anomalies['seller_city'])
    ).astype(int)
    
    # Calculer le taux total d'anomalies
    anomaly_columns = [
        'anomaly_numeric_city', 'anomaly_contains_slashes', 'anomaly_contains_commas',
        'anomaly_contains_brasil', 'anomaly_too_short', 'anomaly_extra_spaces', 'has_canonical_mismatch'
    ]
    
    df_sellers_anomalies['total_anomaly_flags'] = df_sellers_anomalies[anomaly_columns].sum(axis=1)
    
    # Sélectionner les colonnes pertinentes pour le résultat final
    result_columns = [
        'seller_id',
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state',
        'canonical_city_name',
        'total_anomaly_flags'
    ] + anomaly_columns
    
    df_sellers_anomalies_final = df_sellers_anomalies[result_columns].copy()
    
    print(f"\nDonnées des vendeurs avec anomalies: {df_sellers_anomalies_final.shape[0]:,} lignes x {df_sellers_anomalies_final.shape[1]} colonnes")
    
    # Afficher un aperçu des résultats
    print("\nAperçu des premières lignes:")
    print(df_sellers_anomalies_final.head())
    
    # Afficher des statistiques sur les anomalies
    total_sellers = len(df_sellers_anomalies_final)
    sellers_with_any_anomaly = (df_sellers_anomalies_final['total_anomaly_flags'] > 0).sum()
    percentage_with_anomaly = round((sellers_with_any_anomaly / total_sellers) * 100, 2)
    
    print(f"\nStatistiques d'anomalies:")
    print(f"- Total vendeurs: {total_sellers:,}")
    print(f"- Vendeurs avec au moins une anomalie: {sellers_with_any_anomaly:,} ({percentage_with_anomaly}%)")
    
    print(f"\nRépartition des différents types d'anomalies:")
    for col in anomaly_columns:
        count = df_sellers_anomalies_final[col].sum()
        pct = round((count / total_sellers) * 100, 2)
        print(f"- {col}: {count:,} ({pct}%)")
    
    # Sauvegarder les données des vendeurs avec anomalies
    output_path = os.path.join(PROCESSED_DATA_PATH, 'sellers_location_anomalies.csv')
    df_sellers_anomalies_final.to_csv(output_path, index=False)
    print(f"\n[OK] Données des vendeurs avec anomalies sauvegardées: {output_path}")
    
    # Générer un rapport de validation
    anomaly_report = {}
    for col in anomaly_columns:
        anomaly_report[f'{col}_count'] = int(df_sellers_anomalies_final[col].sum())
        anomaly_report[f'{col}_percentage'] = float(round((df_sellers_anomalies_final[col].sum() / total_sellers) * 100, 2))
    
    validation_report = {
        'total_sellers': total_sellers,
        'sellers_with_any_anomaly': int(sellers_with_any_anomaly),
        'anomaly_rate_percent': float(percentage_with_anomaly),
        'average_anomaly_flags_per_seller': float(round(df_sellers_anomalies_final['total_anomaly_flags'].mean(), 2))
    }
    
    # Combiner les deux rapports
    validation_report.update(anomaly_report)
    
    # Sauvegarder le rapport de validation
    sellers_report_dir = os.path.join(REPORTS_PATH, 'sellers')
    os.makedirs(sellers_report_dir, exist_ok=True)
    report_path = os.path.join(sellers_report_dir, 'sellers_anomalies_detection_report.csv')
    report_df = pd.DataFrame([validation_report])
    report_df.to_csv(report_path, index=False)
    print(f"[OK] Rapport de détection d'anomalies sauvegardé: {report_path}")
    
    print(f"\nProcessus de détection d'anomalies terminé avec succès!")
    return df_sellers_anomalies_final

if __name__ == "__main__":
    detect_seller_anomalies()