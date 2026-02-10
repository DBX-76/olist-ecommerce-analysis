"""
Script pour la détection et le nettoyage des anomalies dans les données de commande et de paiement.
Ce script implémente l'analyse complète des anomalies dans order_items et order_payments.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
import yaml

def load_configuration():
    """Charge la configuration depuis le fichier YAML"""
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    RAW_DATA_PATH = config['paths']['raw_data']
    PROCESSED_DATA_PATH = config['paths']['processed_data']
    REPORTS_PATH = config['paths']['reports']
    
    return RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH

def detect_and_clean_anomalies():
    """Détecte et nettoie les anomalies dans les données de commande et de paiement"""
    
    print("="*80)
    print("DÉTECTION ET NETTOYAGE DES ANOMALIES DANS LES COMMANDES ET PAIEMENTS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Créer le répertoire de sortie s'il n'existe pas
    output_dir = Path(PROCESSED_DATA_PATH) / "financial_analysis"
    output_dir.mkdir(exist_ok=True)
    
    # Charger les données
    print("   Chargement des tables...")
    orders_file = os.path.join(RAW_DATA_PATH, 'olist_orders_dataset.csv')
    order_items_file = os.path.join(RAW_DATA_PATH, 'olist_order_items_dataset.csv')
    order_payments_file = os.path.join(RAW_DATA_PATH, 'olist_order_payments_dataset.csv')
    
    orders = pd.read_csv(orders_file)
    order_items = pd.read_csv(order_items_file)
    order_payments = pd.read_csv(order_payments_file)
    
    print(f"   • Orders: {len(orders):,}")
    print(f"   • Order Items: {len(order_items):,} ({order_items['order_id'].nunique():,} commandes)")
    print(f"   • Order Payments: {len(order_payments):,} ({order_payments['order_id'].nunique():,} commandes)")

    # ============================================================================
    # 2. DÉTECTION DES ANOMALIES
    # ============================================================================
    print("\n   Detection des anomalies...")

    # --- Anomalie 1 : Montants non réconciliés ---
    item_totals = order_items.groupby('order_id').agg(
        item_count=('order_item_id', 'count'),
        seller_count=('seller_id', 'nunique'),
        total_price=('price', 'sum'),
        total_freight=('freight_value', 'sum'),
        order_total=('price', lambda x: (x + order_items.loc[x.index, 'freight_value']).sum())
    ).reset_index()

    payment_totals = order_payments.groupby('order_id').agg(
        payment_count=('payment_sequential', 'count'),
        payment_type_count=('payment_type', 'nunique'),
        payment_total=('payment_value', 'sum'),
        payment_types=('payment_type', lambda x: ', '.join(sorted(x.unique()))),
        max_installments=('payment_installments', 'max')
    ).reset_index()

    # Jointure avec orders pour statut
    reconciliation = orders[['order_id', 'order_status']].merge(
        item_totals, on='order_id', how='left'
    ).merge(
        payment_totals, on='order_id', how='left'
    )

    # Calcul des écarts
    reconciliation['item_total'] = reconciliation['order_total'].fillna(0)
    reconciliation['payment_total'] = reconciliation['payment_total'].fillna(0)
    reconciliation['amount_difference'] = (reconciliation['item_total'] - reconciliation['payment_total']).abs()

    # Flags d'anomalies
    reconciliation['anomaly_no_items'] = reconciliation['item_count'].isna()
    reconciliation['anomaly_no_payment'] = reconciliation['payment_count'].isna()
    reconciliation['anomaly_amount_mismatch'] = reconciliation['amount_difference'] > 0.01
    reconciliation['anomaly_many_payments'] = reconciliation['payment_count'] > 10
    reconciliation['anomaly_many_items'] = reconciliation['item_count'] > 15
    reconciliation['anomaly_delivered_no_payment'] = (
        (reconciliation['order_status'] == 'delivered') &
        reconciliation['anomaly_no_payment']
    )

    # --- Anomalie 2 : Paiements suspects ---
    payment_anomalies = order_payments.copy()
    payment_anomalies['anomaly_zero_amount'] = payment_anomalies['payment_value'] == 0
    payment_anomalies['anomaly_negative_amount'] = payment_anomalies['payment_value'] < 0
    payment_anomalies['anomaly_zero_installments'] = (
        (payment_anomalies['payment_type'] == 'credit_card') &
        (payment_anomalies['payment_installments'] == 0)
    )
    payment_anomalies['anomaly_invalid_type'] = payment_anomalies['payment_type'] == 'not_defined'

    # --- Anomalie 3 : Duplications ---
    duplicates_items = order_items.duplicated(subset=['order_id', 'order_item_id'], keep=False)
    duplicates_payments = order_payments.duplicated(
        subset=['order_id', 'payment_sequential'], keep=False
    )

    print(f"   -> {reconciliation['anomaly_amount_mismatch'].sum():,} commandes avec écart montant")
    print(f"   -> {reconciliation['anomaly_delivered_no_payment'].sum()} commandes LIVRÉES sans paiement !")
    print(f"   -> {payment_anomalies['anomaly_zero_amount'].sum()} paiements à 0 $")
    print(f"   -> {duplicates_items.sum()} duplications dans order_items")
    print(f"   -> {duplicates_payments.sum()} duplications dans order_payments")

    # ============================================================================
    # 3. ANALYSE DÉTAILLÉE DES ANOMALIES CRITIQUES
    # ============================================================================
    print("\n   Anomalies Critiques Detectees:")

    # Commande livrée sans paiement
    critical = reconciliation[reconciliation['anomaly_delivered_no_payment']]
    if not critical.empty:
        print(f"\n   COMMANDE LIVREE SANS PAIEMENT:")
        print(critical[['order_id', 'order_status', 'item_total', 'payment_total']].to_string(index=False))
        print("   -> Action requise: Verifier si livraison gratuite/voucher ou erreur systeme")

    # Plus grands écarts
    big_diffs = reconciliation[reconciliation['amount_difference'] > 50].sort_values(
        'amount_difference', ascending=False
    ).head(5)
    if not big_diffs.empty:
        print(f"\n   5 plus grands ecarts montants:")
        print(big_diffs[['order_id', 'item_total', 'payment_total', 'amount_difference']].to_string(index=False))

    # Paiements suspects
    suspicious_payments = payment_anomalies[
        payment_anomalies['anomaly_zero_amount'] |
        payment_anomalies['anomaly_zero_installments'] |
        payment_anomalies['anomaly_invalid_type']
    ]
    if not suspicious_payments.empty:
        print(f"\n   Paiements suspects:")
        print(suspicious_payments[
            ['order_id', 'payment_type', 'payment_value', 'payment_installments']
        ].to_string(index=False))

    # ============================================================================
    # 4. NETTOYAGE & CORRECTION
    # ============================================================================
    print("\n   Nettoyage des donnees...")

    # --- Correction 1 : Supprimer duplications ---
    order_items_clean = order_items[~duplicates_items].copy()
    order_payments_clean = order_payments[~duplicates_payments].copy()
    print(f"   -> Duplications supprimées: {duplicates_items.sum()} dans items, {duplicates_payments.sum()} dans payments")

    # --- Correction 2 : Standardiser types de paiement ---
    order_payments_clean['payment_type'] = order_payments_clean['payment_type'].replace('not_defined', 'voucher')
    print(f"   -> Types 'not_defined' corrigés en 'voucher'")

    # --- Correction 3 : Corriger installments = 0 pour cartes crédit ---
    mask_cc_zero = (order_payments_clean['payment_type'] == 'credit_card') & (order_payments_clean['payment_installments'] == 0)
    order_payments_clean.loc[mask_cc_zero, 'payment_installments'] = 1
    print(f"   -> {mask_cc_zero.sum()} installments corrigés de 0 -> 1 pour cartes crédit")

    # --- Correction 4 : Gérer paiements à 0 $ ---
    # Conserver mais flagger (peuvent être des vouchers gratuits légitimes)
    order_payments_clean['is_free_voucher'] = order_payments_clean['payment_value'] == 0

    # ============================================================================
    # 5. TABLE FINALE OPTIMISÉE
    # ============================================================================
    print("\n   Creation des tables nettoyes...")

    # Ajouter flags d'anomalie aux tables finales pour audit
    order_items_final = order_items_clean.copy()
    order_items_final['is_duplicate'] = False  # Déjà supprimés

    order_payments_final = order_payments_clean.copy()
    order_payments_final['anomaly_zero_amount'] = payment_anomalies.loc[
        ~duplicates_payments, 'anomaly_zero_amount'
    ].values
    order_payments_final['anomaly_zero_installments'] = payment_anomalies.loc[
        ~duplicates_payments, 'anomaly_zero_installments'
    ].values

    # Export
    order_items_output = os.path.join(output_dir, "order_items_clean.csv")
    order_payments_output = os.path.join(output_dir, "order_payments_clean.csv")
    reconciliation_output = os.path.join(output_dir, "order_financial_reconciliation.csv")
    
    order_items_final.to_csv(order_items_output, index=False)
    order_payments_final.to_csv(order_payments_output, index=False)
    reconciliation.to_csv(reconciliation_output, index=False)

    print(f"   -> order_items_clean.csv ({len(order_items_final):,} lignes)")
    print(f"   -> order_payments_clean.csv ({len(order_payments_final):,} lignes)")
    print(f"   -> order_financial_reconciliation.csv ({len(reconciliation):,} lignes)")

    # ============================================================================
    # 6. RAPPORT DE QUALITÉ
    # ============================================================================
    print("\n   RAPPORT DE QUALITE FINAL")
    print("="*70)
    print(f"{'Metrique':<45} {'Valeur':>20}")
    print("-"*70)
    print(f"{'Commandes totales':<45} {len(orders):>20,}")
    print(f"{'Commandes avec items':<45} {reconciliation['item_count'].notna().sum():>20,}")
    print(f"{'Commandes avec paiements':<45} {reconciliation['payment_count'].notna().sum():>20,}")
    print(f"{'Commandes livrees SANS paiement':<45} {reconciliation['anomaly_delivered_no_payment'].sum():>20}")
    print(f"{'Ecarts montants > 0.01$':<45} {reconciliation['anomaly_amount_mismatch'].sum():>20,}")
    print(f"{'Duplications supprimees (items)':<45} {duplicates_items.sum():>20,}")
    print(f"{'Duplications supprimees (payments)':<45} {duplicates_payments.sum():>20,}")
    print(f"{'Paiements a 0$ conserves (vouchers)':<45} {order_payments_final['is_free_voucher'].sum():>20,}")
    print("="*70)

    # Générer un rapport détaillé
    quality_report = {
        'total_orders': len(orders),
        'orders_with_items': reconciliation['item_count'].notna().sum(),
        'orders_with_payments': reconciliation['payment_count'].notna().sum(),
        'delivered_orders_without_payment': reconciliation['anomaly_delivered_no_payment'].sum(),
        'amount_mismatches': reconciliation['anomaly_amount_mismatch'].sum(),
        'duplicates_removed_items': duplicates_items.sum(),
        'duplicates_removed_payments': duplicates_payments.sum(),
        'zero_amount_payments': order_payments_final['is_free_voucher'].sum(),
        'critical_anomalies_investigation_needed': reconciliation['anomaly_delivered_no_payment'].sum()
    }

    # Sauvegarder le rapport de qualité
    report_path = os.path.join(REPORTS_PATH, 'anomaly_detection', 'financial_anomaly_report.csv')
    report_df = pd.DataFrame([quality_report])
    report_df.to_csv(report_path, index=False)
    print(f"\n   Rapport d'anomalie financier sauvegarde: {report_path}")

    print(f"\n   Tables nettoyees exportees dans: {output_dir.absolute()}")
    print("\n   Recommandations:")
    print("   1. INVESTIGUER la commande livrée sans paiement (anomalie critique)")
    print("   2. Documenter les écarts montants > 50$ (remises/taxes non enregistrées)")
    print("   3. Conserver les flags d'anomalie pour audit futur")
    print("   4. Ajouter contrainte CHECK en BDD: payment_value > 0 OR payment_type = 'voucher'")
    
    return order_items_final, order_payments_final, reconciliation

if __name__ == "__main__":
    detect_and_clean_anomalies()