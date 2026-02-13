import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def get_database_url():
    """Retourne l'URL de connexion à la base de données PostgreSQL depuis les variables d'environnement"""
    # First check for complete DATABASE_URL (Neon format)
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url
    
    # Fallback to individual parameters
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    if all([db_host, db_port, db_name, db_user, db_password]):
        url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Ajoute sslmode=require si on est sur Neon (pas en local)
        if db_host != 'localhost':
            url += "?sslmode=require"
        
        return url
    
    raise ValueError("Les variables d'environnement pour la connexion à la base de données ne sont pas toutes définies. Veuillez vérifier votre fichier .env")

def test_connection():
    """Teste la connexion à la base de données"""
    try:
        db_url = get_database_url()
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connexion à la base de données réussie!")
        return True
    except Exception as e:
        print(f"Échec de la connexion à la base de données: {e}")
        return False

def create_tables():
    """Crée les tables dans la base de données"""
    # Tester la connexion avant de créer les tables
    if not test_connection():
        print("Arrêt de l'initialisation de la base de données en raison d'un problème de connexion.")
        return
    
    db_url = get_database_url()
    engine = create_engine(db_url)

    # Supprimer les tables existantes en respectant l'ordre des dépendances
    drop_queries = [
        "DROP TABLE IF EXISTS reviews CASCADE;",
        "DROP TABLE IF EXISTS payments CASCADE;",
        "DROP TABLE IF EXISTS order_items CASCADE;",
        "DROP TABLE IF EXISTS orders CASCADE;",
        "DROP TABLE IF EXISTS products CASCADE;",
        "DROP TABLE IF EXISTS sellers CASCADE;",
        "DROP TABLE IF EXISTS customers CASCADE;"
    ]

    queries = [
        """
        CREATE TABLE customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix INT,
            original_city_name VARCHAR(100),
            standardized_city_name VARCHAR(100),
            customer_state VARCHAR(2),
            city_name_standardized VARCHAR(100),
            avg_latitude DECIMAL(9, 6),
            avg_longitude DECIMAL(9, 6),
            geo_coordinate_samples INT,
            geo_data_quality VARCHAR(50),
            lat_spread_km DECIMAL(9, 2),
            lon_spread_km DECIMAL(9, 2)
        );
        """,
        """
        CREATE TABLE sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix INT,
            original_city_name VARCHAR(100),
            standardized_city_name VARCHAR(100),
            seller_state VARCHAR(2),
            was_standardized BOOLEAN,
            anomaly_numeric_city BOOLEAN,
            anomaly_contains_slashes BOOLEAN,
            anomaly_contains_commas BOOLEAN,
            anomaly_contains_brasil BOOLEAN,
            anomaly_too_short BOOLEAN,
            avg_latitude DECIMAL(9, 6),
            avg_longitude DECIMAL(9, 6),
            geo_coordinate_samples INT,
            geo_data_quality VARCHAR(50),
            lat_spread_km DECIMAL(9, 2),
            lon_spread_km DECIMAL(9, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_category_name_english VARCHAR(100),
            product_name_length INT,
            product_description_length INT,
            product_photos_qty INT,
            product_weight_g INT,
            product_length_cm INT,
            product_height_cm INT,
            product_width_cm INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_status VARCHAR(20),
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            is_gift_order BOOLEAN,
            gift_reason TEXT,
            has_fragmented_vouchers BOOLEAN
            -- FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_id VARCHAR(50),
            order_item_id INT,
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            shipping_limit_date TIMESTAMP,
            price DECIMAL(10, 2),
            freight_value DECIMAL(10, 2),
            icms_value DECIMAL(10, 2),
            is_gift_item BOOLEAN,
            PRIMARY KEY (order_id, order_item_id)
            -- FOREIGN KEY (order_id) REFERENCES orders(order_id),
            -- FOREIGN KEY (product_id) REFERENCES products(product_id),
            -- FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS payments (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(20),
            payment_installments INT,
            payment_value DECIMAL(10, 2),
            is_free_voucher BOOLEAN,
            anomaly_zero_amount BOOLEAN,
            anomaly_zero_installments BOOLEAN
            -- FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS reviews (
            review_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            review_score INT CHECK (review_score BETWEEN 1 AND 5),
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
            -- FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );
        """
    ]
    
    with engine.connect() as conn:
        # Supprimer les tables existantes
        for query in drop_queries:
            conn.execute(text(query))
        
        # Créer les nouvelles tables
        for query in queries:
            conn.execute(text(query))
        
        conn.commit()

    print("Tables créées avec succès dans PostgreSQL.")

if __name__ == "__main__":
    create_tables()