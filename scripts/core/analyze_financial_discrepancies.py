"""
Script pour l'analyse approfondie des écarts financiers entre les commandes et les paiements.
Ce script fournit une analyse détaillée des différences entre les montants des commandes et des paiements.
"""

import pandas as pd
import numpy as np
import os
import yaml
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def load_configuration():
    """Charge la configuration depuis le fichier YAML"""
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    RAW_DATA_PATH = config['paths']['raw_data']
    PROCESSED_DATA_PATH = config['paths']['processed_data']
    REPORTS_PATH = config['paths']['reports']
    
    return RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH

def analyze_financial_discrepancies():
    """Analyse les écarts financiers entre les commandes et les paiements"""
    
    print("="*80)
    print("ANALYSE APPROFONDIE DES ÉCARTS FINANCIERS")
    print("="*80)
    
    # Charger la configuration
    RAW_DATA_PATH, PROCESSED_DATA_PATH, REPORTS_PATH = load_configuration()
    
    # Créer le répertoire de sortie s'il n'existe pas
    output_dir = Path(REPORTS_PATH) / "anomaly_detection" / "financial_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Charger les données
    print("   Chargement des données...")
    
    # Charger les données de base
    orders_file = os.path.join(RAW_DATA_PATH, 'olist_orders_dataset.csv')
    order_items_file = os.path.join(RAW_DATA_PATH, 'olist_order_items_dataset.csv')
    order_payments_file = os.path.join(RAW_DATA_PATH, 'olist_order_payments_dataset.csv')
    
    orders = pd.read_csv(orders_file)
    order_items = pd.read_csv(order_items_file)
    order_payments = pd.read_csv(order_payments_file)
    
    print(f"   • Orders: {len(orders):,} enregistrements")
    print(f"   • Order Items: {len(order_items):,} enregistrements")
    print(f"   • Order Payments: {len(order_payments):,} enregistrements")
    
    # Calculer les totaux par commande
    item_totals = order_items.groupby('order_id').agg(
        item_count=('order_item_id', 'count'),
        total_price=('price', 'sum'),
        total_freight=('freight_value', 'sum'),
        order_total=('price', lambda x: (x + order_items.loc[x.index, 'freight_value']).sum())
    ).reset_index()
    
    payment_totals = order_payments.groupby('order_id').agg(
        payment_count=('payment_sequential', 'count'),
        payment_total=('payment_value', 'sum'),
        payment_types=('payment_type', lambda x: ', '.join(sorted(x.unique()))),
        avg_installments=('payment_installments', 'mean')
    ).reset_index()
    
    # Fusionner les données
    financial_analysis = orders[['order_id', 'order_status', 'order_purchase_timestamp']].merge(
        item_totals, on='order_id', how='left'
    ).merge(
        payment_totals, on='order_id', how='left'
    )
    
    # Calculer les écarts
    financial_analysis['item_total'] = financial_analysis['order_total'].fillna(0)
    financial_analysis['payment_total'] = financial_analysis['payment_total'].fillna(0)
    financial_analysis['amount_difference'] = financial_analysis['item_total'] - financial_analysis['payment_total']
    financial_analysis['abs_difference'] = financial_analysis['amount_difference'].abs()
    
    # Catégoriser les écarts
    def categorize_discrepancy(diff):
        if diff == 0:
            return 'No Discrepancy'
        elif abs(diff) <= 0.01:
            return 'Minor Discrepancy (Rounding)'
        elif abs(diff) <= 5:
            return 'Small Discrepancy'
        elif abs(diff) <= 50:
            return 'Medium Discrepancy'
        else:
            return 'Large Discrepancy'
    
    financial_analysis['discrepancy_category'] = financial_analysis['abs_difference'].apply(categorize_discrepancy)
    
    # Identifier les types d'anomalies
    financial_analysis['anomaly_no_items'] = financial_analysis['item_count'].isna()
    financial_analysis['anomaly_no_payment'] = financial_analysis['payment_count'].isna()
    financial_analysis['anomaly_large_diff'] = financial_analysis['abs_difference'] > 50
    financial_analysis['anomaly_negative_item'] = financial_analysis['item_total'] < 0
    financial_analysis['anomaly_negative_payment'] = financial_analysis['payment_total'] < 0
    
    print(f"\n   Repartition des ecarts:")
    discrepancy_counts = financial_analysis['discrepancy_category'].value_counts()
    for category, count in discrepancy_counts.items():
        percentage = (count / len(financial_analysis)) * 100
        print(f"   - {category}: {count:,} ({percentage:.2f}%)")

    # Analyse des grandes anomalies
    large_discrepancies = financial_analysis[financial_analysis['anomaly_large_diff']].copy()
    if not large_discrepancies.empty:
        print(f"\n   Analyse des {len(large_discrepancies)} grandes anomalies (>50$):")
        print(large_discrepancies[['order_id', 'order_status', 'item_total', 'payment_total', 'amount_difference']].head(10).to_string(index=False))

    # Analyse par statut de commande
    print(f"\n   Analyse par statut de commande:")
    status_analysis = financial_analysis.groupby('order_status').agg({
        'amount_difference': ['count', 'mean', 'std', 'min', 'max'],
        'item_total': 'mean',
        'payment_total': 'mean'
    }).round(2)
    print(status_analysis)

    # Identifier les commandes livrées sans paiement
    delivered_no_payment = financial_analysis[
        (financial_analysis['order_status'] == 'delivered') &
        (financial_analysis['anomaly_no_payment'])
    ]

    if not delivered_no_payment.empty:
        print(f"\n   {len(delivered_no_payment)} commandes LIVREES sans paiement:")
        print(delivered_no_payment[['order_id', 'item_total', 'payment_total']].to_string(index=False))

    # Analyse des tendances temporelles
    financial_analysis['order_month'] = pd.to_datetime(financial_analysis['order_purchase_timestamp']).dt.to_period('M')
    monthly_discrepancies = financial_analysis.groupby('order_month').agg({
        'amount_difference': ['count', 'mean', 'sum'],
        'anomaly_large_diff': 'sum'
    }).round(2)

    print(f"\n   Tendances mensuelles des ecarts:")
    print(monthly_discrepancies.head())

    # Créer des visualisations
    print(f"\n   Generation des visualisations...")

    # Figure 1: Distribution des écarts
    plt.figure(figsize=(12, 8))
    
    plt.subplot(2, 2, 1)
    sns.histplot(financial_analysis['abs_difference'], bins=50, log_scale=(False, True))
    plt.title('Distribution des valeurs absolues des écarts\n(Log scale pour l\'axe Y)')
    plt.xlabel('Valeur absolue de l\'écart ($)')
    plt.ylabel('Nombre de commandes (log scale)')
    
    # Figure 2: Répartition des catégories d'écart
    plt.subplot(2, 2, 2)
    discrepancy_counts.plot(kind='bar')
    plt.title('Répartition des catégories d\'écart')
    plt.xlabel('Catégorie d\'écart')
    plt.ylabel('Nombre de commandes')
    plt.xticks(rotation=45)
    
    # Figure 3: Écarts moyens par statut de commande
    plt.subplot(2, 2, 3)
    status_means = financial_analysis.groupby('order_status')['amount_difference'].mean().sort_values(key=abs, ascending=False)
    status_means.plot(kind='bar')
    plt.title('Écart moyen par statut de commande')
    plt.xlabel('Statut de commande')
    plt.ylabel('Écart moyen ($)')
    plt.xticks(rotation=45)
    
    # Figure 4: Évolution des grandes anomalies dans le temps
    plt.subplot(2, 2, 4)
    if not monthly_discrepancies.empty:
        monthly_large_diff = monthly_discrepancies[('anomaly_large_diff', 'sum')]
        plt.plot(monthly_large_diff.index.astype(str), monthly_large_diff.values)
        plt.title('Évolution des grandes anomalies (>50$) dans le temps')
        plt.xlabel('Mois')
        plt.ylabel('Nombre de grandes anomalies')
        plt.xticks(rotation=45)
    
    plt.tight_layout()
    plot_path = output_dir / "financial_discrepancies_analysis.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   -> Graphiques sauvegardes: {plot_path}")
    
    # Sauvegarder les résultats détaillés
    detailed_results_path = output_dir / "detailed_financial_analysis.csv"
    financial_analysis.to_csv(detailed_results_path, index=False)
    print(f"   -> Resultats detailles sauvegardes: {detailed_results_path}")
    
    # Créer un rapport synthétique
    summary_report = {
        'total_orders': len(financial_analysis),
        'orders_with_discrepancies': len(financial_analysis[financial_analysis['abs_difference'] > 0.01]),
        'large_discrepancies_count': len(large_discrepancies),
        'delivered_no_payment_count': len(delivered_no_payment),
        'total_discrepancy_amount': financial_analysis['amount_difference'].sum(),
        'average_discrepancy': financial_analysis['amount_difference'].mean(),
        'max_discrepancy': financial_analysis['amount_difference'].max(),
        'min_discrepancy': financial_analysis['amount_difference'].min()
    }
    
    summary_report_path = output_dir / "financial_discrepancies_summary.csv"
    summary_df = pd.DataFrame([summary_report])
    summary_df.to_csv(summary_report_path, index=False)
    print(f"   -> Rapport synthetique sauvegarde: {summary_report_path}")
    
    print(f"\n   Analyse approfondie des ecarts financiers terminee!")
    print(f"   Fichiers generes dans: {output_dir}")
    
    return financial_analysis

if __name__ == "__main__":
    analyze_financial_discrepancies()