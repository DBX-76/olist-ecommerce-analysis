"""
Script pour générer un schéma des données traitées
"""
import pandas as pd
import yaml
from pathlib import Path

def generate_processed_data_schema():
    """
    Génère un schéma des données traitées pour la base de données
    """
    print("="*70)
    print("SCHEMA DES DONNÉES TRAITÉES - PRÊT POUR BASE DE DONNÉES")
    print("="*70)
    
    # Charger la configuration
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    PROCESSED_DATA_PATH = config['paths']['processed_data']
    
    print("\n   TABLES PRINCIPALES - DESCRIPTION STRUCTURELLE")
    print("-" * 60)
    
    # Définition des tables et de leurs relations
    tables_info = {
        'zip_code_reference': {
            'description': 'Table de référence géographique canonique',
            'primary_key': 'zip_code_prefix, state',
            'columns': [
                'zip_code_prefix (INT) - Préfixe du code postal',
                'state (VARCHAR) - État brésilien',
                'canonical_city_name (VARCHAR) - Nom canonique de la ville',
                'avg_latitude (DECIMAL) - Latitude moyenne',
                'avg_longitude (DECIMAL) - Longitude moyenne',
                'min_latitude (DECIMAL) - Latitude minimum',
                'max_latitude (DECIMAL) - Latitude maximum',
                'min_longitude (DECIMAL) - Longitude minimum',
                'max_longitude (DECIMAL) - Longitude maximum',
                'latitude_stddev (DECIMAL) - Écart-type latitude',
                'longitude_stddev (DECIMAL) - Écart-type longitude',
                'coordinate_samples (INT) - Nombre d\'échantillons',
                'city_name_variations (INT) - Variations de noms de ville',
                'lat_spread_km (DECIMAL) - Étendue latitude en km',
                'lon_spread_km (DECIMAL) - Étendue longitude en km',
                'data_quality (VARCHAR) - Qualité des données (High/Medium/Low)'
            ],
            'purpose': 'Référence unique pour les coordonnées géographiques par code postal/état'
        },
        
        'customers_with_geolocation': {
            'description': 'Clients enrichis avec géolocalisation',
            'primary_key': 'customer_id',
            'foreign_keys': ['customer_zip_code_prefix, customer_state -> zip_code_reference'],
            'columns': [
                'customer_id (VARCHAR) - ID unique client',
                'customer_unique_id (VARCHAR) - ID unique client (persistant)',
                'customer_zip_code_prefix (INT) - Préfixe code postal client',
                'original_city_name (VARCHAR) - Nom de ville original',
                'standardized_city_name (VARCHAR) - Nom de ville standardisé',
                'customer_state (VARCHAR) - État client',
                'city_name_standardized (BOOLEAN) - Flag ville standardisée',
                'avg_latitude (DECIMAL) - Latitude moyenne du code postal',
                'avg_longitude (DECIMAL) - Longitude moyenne du code postal',
                'coordinate_samples (INT) - Nombre d\'échantillons géo',
                'geo_data_quality (VARCHAR) - Qualité des données géo',
                'lat_spread_km (DECIMAL) - Étendue latitude en km',
                'lon_spread_km (DECIMAL) - Étendue longitude en km'
            ],
            'purpose': 'Données clients avec coordonnées géographiques'
        },
        
        'sellers_with_geolocation': {
            'description': 'Vendeurs enrichis avec géolocalisation',
            'primary_key': 'seller_id',
            'foreign_keys': ['seller_zip_code_prefix, seller_state -> zip_code_reference'],
            'columns': [
                'seller_id (VARCHAR) - ID unique vendeur',
                'seller_zip_code_prefix (INT) - Préfixe code postal vendeur',
                'original_city_name (VARCHAR) - Nom de ville original',
                'standardized_city_name (VARCHAR) - Nom de ville standardisé',
                'seller_state (VARCHAR) - État vendeur',
                'was_standardized (BOOLEAN) - Flag standardisation effectuée',
                'anomaly_numeric_city (BOOLEAN) - Anomalie: ville numérique',
                'anomaly_contains_slashes (BOOLEAN) - Anomalie: slash dans nom',
                'anomaly_contains_commas (BOOLEAN) - Anomalie: virgule dans nom',
                'anomaly_contains_brasil (BOOLEAN) - Anomalie: contient "brasil"',
                'anomaly_too_short (BOOLEAN) - Anomalie: nom trop court',
                'avg_latitude (DECIMAL) - Latitude moyenne du code postal',
                'avg_longitude (DECIMAL) - Longitude moyenne du code postal',
                'geo_coordinate_samples (INT) - Nombre d\'échantillons géo',
                'geo_data_quality (VARCHAR) - Qualité des données géo',
                'lat_spread_km (DECIMAL) - Étendue latitude en km',
                'lon_spread_km (DECIMAL) - Étendue longitude en km'
            ],
            'purpose': 'Données vendeurs avec coordonnées géographiques et anomalies détectées'
        },
        
        'order_financial_reconciliation': {
            'description': 'Réconciliation financière des commandes',
            'primary_key': 'order_id',
            'columns': [
                'order_id (VARCHAR) - ID unique commande',
                'order_status (VARCHAR) - Statut de la commande',
                'item_count (INT) - Nombre d\'items dans la commande',
                'seller_count (INT) - Nombre de vendeurs différents',
                'total_price (DECIMAL) - Prix total des items',
                'total_freight (DECIMAL) - Frais de port totaux',
                'order_total (DECIMAL) - Total commande (prix + fret)',
                'payment_count (INT) - Nombre de paiements',
                'payment_type_count (INT) - Nombre de types de paiement',
                'payment_total (DECIMAL) - Total payé',
                'payment_types (VARCHAR) - Types de paiement utilisés',
                'max_installments (INT) - Max d\'installments',
                'item_total (DECIMAL) - Total items (copie)',
                'payment_total (DECIMAL) - Total paiement (copie)',
                'amount_difference (DECIMAL) - Différence entre total et paiement',
                'anomaly_no_items (BOOLEAN) - Anomalie: commande sans items',
                'anomaly_no_payment (BOOLEAN) - Anomalie: commande sans paiement',
                'anomaly_amount_mismatch (BOOLEAN) - Anomalie: montants non réconciliés',
                'anomaly_many_payments (BOOLEAN) - Anomalie: beaucoup de paiements',
                'anomaly_many_items (BOOLEAN) - Anomalie: beaucoup d\'items',
                'anomaly_delivered_no_payment (BOOLEAN) - Anomalie: livré sans paiement'
            ],
            'purpose': 'Analyse financière et réconciliation commande/paiement'
        }
    }
    
    # Afficher les informations pour chaque table
    for table_name, info in tables_info.items():
        print(f"\n   {table_name.upper()}")
        print(f"   Description: {info['description']}")
        print(f"   Clé primaire: {info['primary_key']}")
        if 'foreign_keys' in info:
            print(f"   Clés étrangères: {', '.join(info['foreign_keys'])}")
        print(f"   Objectif: {info['purpose']}")
        print("   Colonnes:")
        for col in info['columns']:
            print(f"     - {col}")
    
    print("\n" + "="*70)
    print("RELATIONS ENTRE TABLES")
    print("="*70)
    
    relationships = [
        "zip_code_reference <- customers_with_geolocation (zip_code_prefix, state)",
        "zip_code_reference <- sellers_with_geolocation (zip_code_prefix, state)",
        "orders (à créer) <- order_financial_reconciliation (order_id)"
    ]
    
    for rel in relationships:
        print(f"   {rel}")
    
    print("\n" + "="*70)
    print("CARACTÉRISTIQUES DES DONNÉES TRAITÉES")
    print("="*70)
    
    characteristics = [
        "   Données géographiques standardisées et enrichies",
        "   Anomalies détectées et documentées",
        "   Réconciliation financière effectuée",
        "   Qualité des données mesurée et documentée",
        "   Relations logiques entre tables identifiées",
        "   Prêtes pour ingestion dans une base de données relationnelle"
    ]
    
    for char in characteristics:
        print(f"   {char}")
    
    print("\n   PRÊT POUR CONCEPTION DE LA BASE DE DONNÉES")
    print("="*70)
    
    # Sauvegarder le schéma dans un fichier
    schema_file = Path(PROCESSED_DATA_PATH) / "database_schema_documentation.txt"
    with open(schema_file, 'w', encoding='utf-8') as f:
        f.write("SCHEMA DES DONNÉES TRAITÉES - PRÊT POUR BASE DE DONNÉES\n")
        f.write("="*70 + "\n\n")
        
        for table_name, info in tables_info.items():
            f.write(f"{table_name.upper()}\n")
            f.write(f"Description: {info['description']}\n")
            f.write(f"Clé primaire: {info['primary_key']}\n")
            if 'foreign_keys' in info:
                f.write(f"Clés étrangères: {', '.join(info['foreign_keys'])}\n")
            f.write(f"Objectif: {info['purpose']}\n")
            f.write("Colonnes:\n")
            for col in info['columns']:
                f.write(f"  - {col}\n")
            f.write("\n")
    
    print(f"\n   Schema sauvegardé dans: {schema_file}")
    
    return tables_info

if __name__ == "__main__":
    generate_processed_data_schema()