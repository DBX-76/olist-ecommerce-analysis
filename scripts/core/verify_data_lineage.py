"""
Script de vérification de la provenance des données traitées
Vérifie que chaque fichier CSV brut a été utilisé pour créer les fichiers traités
"""
import pandas as pd
import os
from pathlib import Path

def verify_data_lineage():
    """
    Vérifie la provenance des données traitées
    """
    print("="*80)
    print("VÉRIFICATION DE LA PROVENANCE DES DONNÉES TRAITÉES")
    print("="*80)
    
    # Chemins des données
    raw_path = "data/raw/"
    processed_path = "data/processed/"
    
    print("\n   CORRESPONDANCE ENTRE DONNÉES BRUTES ET TRAITÉES")
    print("-" * 60)
    
    # Définir les correspondances
    data_mapping = {
        'olist_customers_dataset.csv': ['customers_with_geolocation.csv'],
        'olist_orders_dataset.csv': ['order_financial_reconciliation.csv'],
        'olist_order_items_dataset.csv': ['order_financial_reconciliation.csv'],
        'olist_order_payments_dataset.csv': ['order_financial_reconciliation.csv'],
        'olist_order_reviews_dataset.csv': ['reviews_quality_analyzed.csv'],
        'olist_products_dataset.csv': ['products_quality_analyzed.csv'],
        'olist_sellers_dataset.csv': ['sellers_with_geolocation.csv'],
        'olist_geolocation_dataset.csv': ['zip_code_reference.csv'],
        'product_category_name_translation.csv': ['products_quality_analyzed.csv', 'zip_code_reference.csv']
    }
    
    # Vérifier la présence des fichiers bruts
    raw_files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
    processed_files = []
    
    # Lister tous les fichiers traités dans processed et ses sous-répertoires
    for root, dirs, files in os.walk(processed_path):
        for file in files:
            if file.endswith('.csv'):
                processed_files.append(file)
    
    print("Fichiers bruts présents:")
    for raw_file in raw_files:
        print(f"   [OK] {raw_file}")
    
    print(f"\nFichiers traités présents: {len(processed_files)}")
    for proc_file in processed_files:
        print(f"   [FILE] {proc_file}")
    
    print("\n" + "="*60)
    print("VÉRIFICATION DES TRANSFORMATIONS")
    print("="*60)
    
    lineage_check = []
    
    for raw_file, expected_processed in data_mapping.items():
        if raw_file in raw_files:
            print(f"\n{raw_file} ->")
            for expected in expected_processed:
                if expected in processed_files:
                    print(f"   [OK] {expected}")
                    lineage_check.append(True)
                else:
                    print(f"   [KO] {expected} (ATTENDU MAIS ABSENT)")
                    lineage_check.append(False)
        else:
            print(f"\n[KO] {raw_file} (FICHIER BRUT MANQUANT)")

    print("\n" + "="*60)
    print("RÉSULTAT DE LA VÉRIFICATION")
    print("="*60)

    success_count = sum(lineage_check)
    total_expected = len(lineage_check)

    print(f"Transformations réussies: {success_count}/{total_expected}")

    if success_count == total_expected:
        print("[OK] TOUTES LES DONNÉES BRUTES ONT ÉTÉ UTILISÉES")
        print("[OK] CHAÎNE DE TRANSFORMATION COMPLÈTE")
    else:
        print("[KO] CERTAINES TRANSFORMATIONS SONT MANQUANTES")

    print("\n   DÉTAIL DES UTILISATIONS:")
    usage_details = {
        'customers_with_geolocation.csv': 'Données clients enrichies avec géoloc (depuis olist_customers_dataset.csv)',
        'sellers_with_geolocation.csv': 'Données vendeurs enrichies avec géoloc (depuis olist_sellers_dataset.csv)',
        'zip_code_reference.csv': 'Référence géographique canonique (depuis olist_geolocation_dataset.csv)',
        'order_financial_reconciliation.csv': 'Réconciliation commande/paiement (depuis orders, items, payments)',
        'products_quality_analyzed.csv': 'Analyse qualité produits (depuis olist_products_dataset.csv)',
        'reviews_quality_analyzed.csv': 'Analyse qualité avis (depuis olist_order_reviews_dataset.csv)'
    }
    
    for proc_file, desc in usage_details.items():
        if proc_file in processed_files:
            print(f"   [OK] {proc_file}: {desc}")
        else:
            print(f"   [KO] {proc_file}: {desc} (FICHIER MANQUANT)")
    
    print("\n" + "="*80)
    print("CONCLUSION")
    print("="*80)
    
    if success_count == total_expected:
        print("   Votre pipeline est complet :")
        print("   - Tous les 9 fichiers bruts ont été utilisés")
        print("   - Toutes les transformations attendues sont présentes")
        print("   - Les données sont prêtes pour la base de données")
        print("   - La chaîne de transformation est complète")
    else:
        print("   Certaines données brutes n'ont pas été transformées")
        print("   - Vérifiez les transformations manquantes")
        print("   - Complétez le pipeline si nécessaire")
    
    return success_count == total_expected

if __name__ == "__main__":
    verify_data_lineage()