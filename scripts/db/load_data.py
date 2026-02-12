import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv

# Ajouter le répertoire racine du projet au PATH pour pouvoir importer les modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def get_database_url():
    """Retourne l'URL de connexion à la base de données PostgreSQL depuis les variables d'environnement"""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'olist_db')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'Padmee.P.76*')
    
    url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Ajoute sslmode=require si on est sur Neon (pas en local)
    if db_host != 'localhost':
        url += "?sslmode=require"
    
    return url

def load_data_to_db():
    # Connexion à la base de données
    db_url = os.getenv("DATABASE_URL", get_database_url())
    engine = create_engine(db_url)

    # Supprimer les tables existantes en respectant l'ordre des dépendances
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS reviews CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS payments CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS order_items CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS orders CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS products CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS sellers CASCADE;'))
        conn.execute(text('DROP TABLE IF EXISTS customers CASCADE;'))
        conn.commit()

    # Recréer les tables avec la structure correcte
    from scripts.db.init_db import create_tables
    create_tables()

    # Chargement des CSV
    df_customers = pd.read_csv('data/processed/customers_with_geolocation.csv')
    df_orders = pd.read_csv('data/processed/olist_orders_clean.csv')
    df_payments = pd.read_csv('data/processed/olist_order_payments_dataset_clean.csv')
    df_products = pd.read_csv('data/processed/products_with_translations.csv')
    df_reviews = pd.read_csv('data/processed/olist_order_reviews_clean.csv').drop_duplicates(subset=['review_id'])
    df_sellers = pd.read_csv('data/processed/sellers_with_geolocation.csv')
    # Convertir les colonnes de type entier (0/1) en boolean
    boolean_columns = ['was_standardized', 'anomaly_numeric_city', 'anomaly_contains_slashes', 
                      'anomaly_contains_commas', 'anomaly_contains_brasil', 'anomaly_too_short']
    for col in boolean_columns:
        df_sellers[col] = df_sellers[col].astype(bool)
    df_order_items = pd.read_csv('data/processed/olist_order_items_dataset_clean.csv')

    # Insertion dans les tables (remplacer le contenu)
    df_customers.to_sql('customers', engine, if_exists='append', index=False)
    df_orders.to_sql('orders', engine, if_exists='append', index=False)
    df_payments.to_sql('payments', engine, if_exists='append', index=False)
    df_products.to_sql('products', engine, if_exists='append', index=False)
    df_reviews.to_sql('reviews', engine, if_exists='append', index=False)
    df_sellers.to_sql('sellers', engine, if_exists='append', index=False)
    df_order_items.to_sql('order_items', engine, if_exists='append', index=False)

    print("Données chargées avec succès dans PostgreSQL.")

if __name__ == "__main__":
    load_data_to_db()