"""
Script de nettoyage et d'analyse des anomalies dans les tables products et order_reviews.
Ce script implémente les recommandations de l'analyse approfondie des anomalies produits et avis.
"""

import pandas as pd
import numpy as np
import os
import yaml
from pathlib import Path

def load_configuration():
    """Charge la configuration depuis le fichier YAML"""
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    RAW_DATA_PATH = config['paths']['raw_data']
    PROCESSED_DATA_PATH = config['paths']['processed_data']
    REPORTS_PATH = config['paths']['reports']
    
    return RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH

def analyze_and_clean_products_reviews():
    """Analyse et nettoie les anomalies dans les tables products et order_reviews"""
    
    print("="*80)
    print("ANALYSE ET NETTOYAGE DES ANOMALIES PRODUITS ET AVIS CLIENTS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Créer le répertoire de sortie s'il n'existe pas
    output_dir = Path(PROCESSED_DATA_PATH) / "product_review_analysis"
    output_dir.mkdir(exist_ok=True)
    
    # Charger les données
    print("   Chargement des données...")
    
    # Charger les données de base
    products_file = os.path.join(RAW_DATA_PATH, 'olist_products_dataset.csv')
    reviews_file = os.path.join(RAW_DATA_PATH, 'olist_order_reviews_dataset.csv')
    orders_file = os.path.join(RAW_DATA_PATH, 'olist_orders_dataset.csv')
    translation_file = os.path.join(RAW_DATA_PATH, 'product_category_name_translation.csv')
    
    products = pd.read_csv(products_file)
    reviews = pd.read_csv(reviews_file)
    orders = pd.read_csv(orders_file)
    translation = pd.read_csv(translation_file)
    
    print(f"   - Products: {len(products):,} enregistrements")
    print(f"   - Order Reviews: {len(reviews):,} enregistrements")
    print(f"   - Orders: {len(orders):,} enregistrements")
    print(f"   - Category Translation: {len(translation):,} enregistrements")
    
    # Sauvegarder les données originales avant modification
    original_products = products.copy()
    original_reviews = reviews.copy()
    
    # ============================================================================
    # 1. ANALYSE DES ANOMALIES PRODUITS
    # ============================================================================
    print("\n   ANALYSE 1: Anomalies dans les produits")
    
    # Calculer les volumes et densités
    products['volume_cm3'] = np.nan
    mask_volume = (
        (products['product_length_cm'] > 0) & 
        (products['product_height_cm'] > 0) & 
        (products['product_width_cm'] > 0)
    )
    products.loc[mask_volume, 'volume_cm3'] = (
        products.loc[mask_volume, 'product_length_cm'] *
        products.loc[mask_volume, 'product_height_cm'] *
        products.loc[mask_volume, 'product_width_cm']
    )
    
    products['density_g_per_cm3'] = np.nan
    mask_density = (
        (products['product_weight_g'] > 0) & 
        (products['volume_cm3'] > 0)
    )
    products.loc[mask_density, 'density_g_per_cm3'] = (
        products.loc[mask_density, 'product_weight_g'] / products.loc[mask_density, 'volume_cm3']
    )
    
    # Identifier les catégories digitales
    DIGITAL_CATEGORIES = [
        'cds_dvds_musicals', 'dvds_blu_ray', 'electronics', 'audio',
        'telephony', 'computers', 'computers_accessories', 'pc_gamer'
    ]
    
    products['is_digital_product'] = products['product_category_name'].isin(DIGITAL_CATEGORIES)
    
    # Détecter les anomalies produits
    products['anomaly_missing_physical_dimensions'] = (
        (~products['is_digital_product']) &
        ((products['product_length_cm'].isna()) |
         (products['product_height_cm'].isna()) |
         (products['product_width_cm'].isna()))
    )
    
    products['anomaly_zero_weight_physical_product'] = (
        (~products['is_digital_product']) &
        (products['product_weight_g'] == 0)
    )
    
    products['anomaly_implausible_density'] = (
        products['density_g_per_cm3'] > 20.0  # > Osmium (22.6 g/cm³)
    )
    
    products['anomaly_suspiciously_low_density'] = (
        (products['density_g_per_cm3'] < 0.001) & 
        (products['product_weight_g'] > 100)  # Moins dense que l'air mais lourd
    )
    
    products['anomaly_no_photos'] = (
        products['product_photos_qty'].isna() | 
        (products['product_photos_qty'] == 0)
    )
    
    products['anomaly_missing_translation'] = products['product_category_name'].isna()
    
    # Afficher les statistiques
    print(f"   - Produits avec dimensions manquantes: {products['anomaly_missing_physical_dimensions'].sum():,}")
    print(f"   - Produits avec poids zéro (physiques): {products['anomaly_zero_weight_physical_product'].sum():,}")
    print(f"   - Produits avec densité implausible: {products['anomaly_implausible_density'].sum():,}")
    print(f"   - Produits avec densité suspecte: {products['anomaly_suspiciously_low_density'].sum():,}")
    print(f"   - Produits sans photos: {products['anomaly_no_photos'].sum():,}")
    
    # ============================================================================
    # 2. CORRECTION DES UNITÉS ERRONÉES (mm vs cm, kg vs g)
    # ============================================================================
    print("\n   CORRECTION 2: Correction des unités erronées")
    
    # Détecter les unités erronées
    products['suspected_mm_units'] = (
        (products['density_g_per_cm3'] > 50000) &  # Très haute densité
        (products['product_length_cm'] < 5) &      # Petites dimensions
        (products['product_length_cm'] > 0)        # Mais pas nulles
    )
    
    products['suspected_kg_weight'] = (
        (products['density_g_per_cm3'] < 10) &     # Très basse densité
        (products['product_weight_g'] > 1000) &    # Mais poids élevé
        (products['product_weight_g'] > 0)         # Et pas nul
    )
    
    print(f"   - Produits suspects en mm: {products['suspected_mm_units'].sum():,}")
    print(f"   - Produits suspects en kg: {products['suspected_kg_weight'].sum():,}")
    
    # Correction des unités
    products.loc[products['suspected_mm_units'], 'product_length_cm'] *= 10
    products.loc[products['suspected_mm_units'], 'product_height_cm'] *= 10
    products.loc[products['suspected_mm_units'], 'product_width_cm'] *= 10
    
    # Recalculer les volumes et densités après correction
    products.loc[products['suspected_mm_units'], 'volume_cm3'] = (
        products.loc[products['suspected_mm_units'], 'product_length_cm'] *
        products.loc[products['suspected_mm_units'], 'product_height_cm'] *
        products.loc[products['suspected_mm_units'], 'product_width_cm']
    )
    
    products.loc[products['suspected_mm_units'], 'density_g_per_cm3'] = (
        products.loc[products['suspected_mm_units'], 'product_weight_g'] /
        products.loc[products['suspected_mm_units'], 'volume_cm3']
    )
    
    products.loc[products['suspected_kg_weight'], 'product_weight_g'] *= 1000
    
    # Recalculer les densités après correction du poids
    mask_new_density = (
        (products['product_weight_g'] > 0) & 
        (products['volume_cm3'] > 0) &
        (products['suspected_kg_weight'])
    )
    products.loc[mask_new_density, 'density_g_per_cm3'] = (
        products.loc[mask_new_density, 'product_weight_g'] / 
        products.loc[mask_new_density, 'volume_cm3']
    )
    
    print(f"   - Unités corrigées pour {products['suspected_mm_units'].sum() + products['suspected_kg_weight'].sum():,} produits")
    
    # ============================================================================
    # 3. ANALYSE DES ANOMALIES AVIS CLIENTS
    # ============================================================================
    print("\n   ANALYSE 3: Anomalies dans les avis clients")
    
    # Convertir les dates pour les comparaisons temporelles
    reviews['review_creation_date'] = pd.to_datetime(reviews['review_creation_date'], errors='coerce')
    reviews['review_answer_timestamp'] = pd.to_datetime(reviews['review_answer_timestamp'], errors='coerce')
    
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'], errors='coerce')
    orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'], errors='coerce')
    
    # Fusionner les données pour l'analyse temporelle
    reviews_with_orders = reviews.merge(
        orders[['order_id', 'order_status', 'order_purchase_timestamp', 'order_delivered_customer_date']],
        on='order_id',
        how='left'
    )
    
    # Calculer les écarts temporels
    reviews_with_orders['response_days'] = np.nan
    mask_response = reviews_with_orders['review_answer_timestamp'].notna()
    reviews_with_orders.loc[mask_response, 'response_days'] = (
        reviews_with_orders.loc[mask_response, 'review_answer_timestamp'] -
        reviews_with_orders.loc[mask_response, 'review_creation_date']
    ).dt.total_seconds() / (24 * 3600)
    
    reviews_with_orders['days_after_delivery'] = np.nan
    mask_delivery = reviews_with_orders['order_delivered_customer_date'].notna()
    reviews_with_orders.loc[mask_delivery, 'days_after_delivery'] = (
        reviews_with_orders.loc[mask_delivery, 'review_creation_date'] -
        reviews_with_orders.loc[mask_delivery, 'order_delivered_customer_date']
    ).dt.total_seconds() / (24 * 3600)
    
    # Détecter les anomalies avis
    # Calculer chaque condition séparément pour éviter les erreurs
    before_purchase_mask = reviews_with_orders['review_creation_date'] < (
        reviews_with_orders['order_purchase_timestamp'] - pd.Timedelta(hours=1)
    )
    reviews_with_orders['anomaly_review_before_purchase'] = before_purchase_mask
    
    cancellation_mask = reviews_with_orders['order_status'] == 'canceled'
    negative_score_mask = reviews_with_orders['review_score'] <= 2
    reviews_with_orders['anomaly_negative_review_after_cancellation'] = (
        cancellation_mask & negative_score_mask
    )
    
    no_title_mask = reviews_with_orders['review_comment_title'].isna()
    no_message_mask = reviews_with_orders['review_comment_message'].isna()
    reviews_with_orders['is_silent_review'] = no_title_mask & no_message_mask
    
    reviews_with_orders['no_seller_response'] = reviews_with_orders['review_answer_timestamp'].isna()
    
    slow_response_mask = reviews_with_orders['response_days'] > 30
    valid_response_time_mask = reviews_with_orders['response_days'].notna()
    reviews_with_orders['slow_seller_response'] = slow_response_mask & valid_response_time_mask
    
    status_created_approved_canceled = reviews_with_orders['order_status'].isin(['created', 'approved', 'canceled'])
    not_delivered_mask = reviews_with_orders['order_status'] != 'delivered'
    not_shipped_mask = reviews_with_orders['order_status'] != 'shipped'
    reviews_with_orders['review_before_shipping'] = (
        status_created_approved_canceled & not_delivered_mask & not_shipped_mask
    )
    
    # Afficher les statistiques
    print(f"   - Avis avant achat: {reviews_with_orders['anomaly_review_before_purchase'].sum():,}")
    print(f"   - Avis négatifs après annulation: {reviews_with_orders['anomaly_negative_review_after_cancellation'].sum():,}")
    print(f"   - Avis silencieux: {reviews_with_orders['is_silent_review'].sum():,}")
    print(f"   - Avis sans réponse vendeur: {reviews_with_orders['no_seller_response'].sum():,}")
    print(f"   - Réponses lentes (>30j): {reviews_with_orders['slow_seller_response'].sum():,}")
    
    # ============================================================================
    # 4. CORRECTION DES ANOMALIES CRITIQUES
    # ============================================================================
    print("\n   CORRECTION 4: Correction des anomalies critiques")
    
    # Identifier les 3 cas critiques d'avis avant achat
    critical_reviews = reviews_with_orders[reviews_with_orders['anomaly_review_before_purchase']]
    print(f"   - Avis critiques (avant achat): {len(critical_reviews)}")
    
    if len(critical_reviews) > 0:
        print("   - Détails des cas critiques:")
        for _, row in critical_reviews.head(5).iterrows():
            print(f"     - Order: {row['order_id'][:8]}..., Review: {row['review_id'][:8]}..., "
                  f"Ecart: {row['review_creation_date'] - row['order_purchase_timestamp']}")
    
    # ============================================================================
    # 5. EXPORT DES DONNÉES ANALYSÉES
    # ============================================================================
    print("\n   EXPORT DES DONNÉES ANALYSÉES...")
    
    # Sauvegarder les données produits analysées
    products_output = os.path.join(output_dir, "products_quality_analyzed.csv")
    products.to_csv(products_output, index=False)
    
    # Sauvegarder les données avis analysées
    reviews_output = os.path.join(output_dir, "reviews_quality_analyzed.csv")
    reviews_with_orders.to_csv(reviews_output, index=False)
    
    print(f"   - products_quality_analyzed.csv: {len(products):,} lignes")
    print(f"   - reviews_quality_analyzed.csv: {len(reviews_with_orders):,} lignes")
    
    # ============================================================================
    # 6. RAPPORT D'ANALYSE
    # ============================================================================
    print("\n   RAPPORT D'ANALYSE DES ANOMALIES PRODUITS/AVIS")
    print("="*70)
    print(f"{'Métrique':<45} {'Valeur':>20}")
    print("-"*70)
    print(f"{'Produits analysés':<45} {len(products):>20,}")
    print(f"{'Avis analysés':<45} {len(reviews_with_orders):>20,}")
    print(f"{'Produits avec dimensions manquantes':<45} {products['anomaly_missing_physical_dimensions'].sum():>20,}")
    print(f"{'Produits avec densité implausible':<45} {products['anomaly_implausible_density'].sum():>20,}")
    print(f"{'Avis avant achat (anomalie critique)':<45} {reviews_with_orders['anomaly_review_before_purchase'].sum():>20,}")
    print(f"{'Avis silencieux':<45} {reviews_with_orders['is_silent_review'].sum():>20,}")
    print(f"{'Unités corrigées (mm/kg)':<45} {products['suspected_mm_units'].sum() + products['suspected_kg_weight'].sum():>20,}")
    print("="*70)
    
    # Générer un rapport détaillé
    analysis_report = {
        'total_products': len(products),
        'total_reviews': len(reviews_with_orders),
        'products_missing_dimensions': int(products['anomaly_missing_physical_dimensions'].sum()),
        'products_implausible_density': int(products['anomaly_implausible_density'].sum()),
        'products_suspected_mm_units': int(products['suspected_mm_units'].sum()),
        'products_suspected_kg_weight': int(products['suspected_kg_weight'].sum()),
        'reviews_before_purchase': int(reviews_with_orders['anomaly_review_before_purchase'].sum()),
        'silent_reviews': int(reviews_with_orders['is_silent_review'].sum()),
        'negative_reviews_after_cancellation': int(reviews_with_orders['anomaly_negative_review_after_cancellation'].sum()),
        'no_seller_responses': int(reviews_with_orders['no_seller_response'].sum()),
        'slow_responses_over_30d': int(reviews_with_orders['slow_seller_response'].sum())
    }
    
    # Sauvegarder le rapport d'analyse
    report_path = os.path.join(REPORTS_PATH, 'anomaly_detection', 'product_review_analysis_report.csv')
    report_df = pd.DataFrame([analysis_report])
    report_df.to_csv(report_path, index=False)
    print(f"\n   Rapport d'analyse produits/avis sauvegardé: {report_path}")
    
    print(f"\n   Analyse des anomalies produits/avis terminée!")
    print(f"   Données analysées exportées dans: {output_dir}")
    
    # Afficher les principales découvertes
    print(f"\n   PRINCIPALES DÉCOUVERTES:")
    print(f"   1. {products['anomaly_missing_physical_dimensions'].sum():,} produits avec dimensions manquantes")
    print(f"   2. {products['anomaly_implausible_density'].sum():,} produits avec densité suspecte")
    print(f"   3. {reviews_with_orders['anomaly_review_before_purchase'].sum():,} avis critiques (avant achat)")
    print(f"   4. {reviews_with_orders['is_silent_review'].sum():,} avis silencieux (comportement normal)")
    print(f"   5. {int(products['suspected_mm_units'].sum() + products['suspected_kg_weight'].sum()):,} unités corrigées")
    
    return products, reviews_with_orders

if __name__ == "__main__":
    analyze_and_clean_products_reviews()