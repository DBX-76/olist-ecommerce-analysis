"""
Script de test pour vérifier le bon fonctionnement de l'analyse d'anomalies.
Ce script vérifie que les principaux composants du pipeline fonctionnent correctement.
"""

import pandas as pd
import sys
import os
from pathlib import Path

def test_pipeline():
    """Test le fonctionnement basique du pipeline d'analyse d'anomalies"""
    
    print("="*60)
    print("TEST DU PIPELINE D'ANALYSE D'ANOMALIES")
    print("="*60)
    
    # Vérifier que les répertoires nécessaires existent
    required_dirs = [
        "data/processed",
        "scripts/transform_csv_dataset",
        "reports/anomaly_detection"
    ]
    
    print("1. Verification des repertoires...")
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            print(f"   OK {dir_path}")
        else:
            print(f"   ERREUR {dir_path} - NON TROUVE")
            return False
    
    # Vérifier que les fichiers de sortie principaux existent
    expected_files = [
        "data/processed/zip_code_reference.csv",
        "data/processed/customers_with_geolocation.csv",
        "data/processed/sellers_with_geolocation.csv",
        "data/processed/financial_analysis/order_financial_reconciliation.csv",
        "reports/anomaly_detection/financial_anomaly_report.csv"
    ]
    
    print("\n2. Verification des fichiers de sortie...")
    missing_files = []
    for file_path in expected_files:
        if os.path.exists(file_path):
            print(f"   OK {file_path}")
        else:
            print(f"   ERREUR {file_path} - MANQUANT")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n   WARNING {len(missing_files)} fichiers manquants. Le pipeline n'est peut-etre pas completement execute.")
    
    # Vérifier que les scripts principaux existent
    core_scripts = [
        "scripts/transform_csv_dataset/create_zip_code_reference.py",
        "scripts/transform_csv_dataset/detect_clean_financial_anomalies.py",
        "scripts/transform_csv_dataset/analyze_clean_products_reviews.py"
    ]
    
    print("\n3. Verification des scripts principaux...")
    for script in core_scripts:
        if os.path.exists(script):
            print(f"   OK {script}")
        else:
            print(f"   ERREUR {script} - MANQUANT")
    
    # Tester la lecture d'un fichier de sortie pour vérifier qu'il est valide
    print("\n4. Test de lecture d'un fichier de sortie...")
    try:
        if os.path.exists("data/processed/zip_code_reference.csv"):
            df_test = pd.read_csv("data/processed/zip_code_reference.csv")
            print(f"   OK Lecture reussie: {len(df_test)} lignes, {len(df_test.columns)} colonnes")
            print(f"      Colonnes: {list(df_test.columns)[:5]}{'...' if len(df_test.columns) > 5 else ''}")
        else:
            print("   ERREUR Impossible de lire le fichier de test")
    except Exception as e:
        print(f"   ERREUR Erreur lors de la lecture: {str(e)}")
    
    print("\n" + "="*60)
    print("TEST TERMINÉ")
    print("="*60)
    print("\nInstructions:")
    print("- Si des fichiers sont manquants, exécutez les scripts correspondants")
    print("- Les scripts principaux sont dans le répertoire scripts/transform_csv_dataset/")
    print("- Les rapports sont générés dans le répertoire reports/")
    print("- Les données traitées sont dans le répertoire data/processed/")
    
    return True

if __name__ == "__main__":
    test_pipeline()
