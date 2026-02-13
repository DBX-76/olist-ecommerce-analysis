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

def get_customer_order_interval():
    """
    Récupère les commandes des clients et calcule la différence en jours entre deux commandes consécutives.
    
    Returns:
        pandas.DataFrame: DataFrame contenant le customer_unique_id, order_id, order_purchase_timestamp et la différence en jours entre la commande actuelle et la précédente
    """
    try:
        db_url = get_database_url()
        engine = create_engine(db_url)
        
        # Requête SQL avec fonction LAG()
        query = text("""
            SELECT 
                c.customer_unique_id,
                o.order_id,
                o.order_purchase_timestamp,
                LAG(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp) AS previous_order_timestamp,
                DATE_PART('day', o.order_purchase_timestamp - LAG(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp)) AS days_between_orders
            FROM 
                orders o
            JOIN 
                customers c ON o.customer_id = c.customer_id
            ORDER BY 
                c.customer_unique_id, o.order_purchase_timestamp;
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        print(f"Intervales entre commandes récupérés avec succès!")
        print(f"Nombre de commandes: {len(df)}")
        print(f"Nombre de clients avec au moins 2 commandes: {df[df['days_between_orders'].notna()]['customer_unique_id'].nunique()}")
        print(f"Interval moyen entre commandes: {df['days_between_orders'].mean():.2f} jours")
        
        return df
        
    except Exception as e:
        print(f"Erreur lors de la récupération des intervales entre commandes: {e}")
        return None

def save_to_csv(df, output_path):
    """
    Sauvegarde le DataFrame dans un fichier CSV.
    
    Args:
        df (pandas.DataFrame): DataFrame à sauvegarder
        output_path (str): Chemin du fichier de sortie
    """
    try:
        df.to_csv(output_path, index=False)
        print(f"Intervales entre commandes sauvegardés avec succès dans {output_path}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du fichier: {e}")

if __name__ == "__main__":
    # Récupérer les intervales entre commandes
    order_interval_df = get_customer_order_interval()
    
    # Sauvegarder dans un fichier CSV
    if order_interval_df is not None:
        output_dir = 'reports/cte'
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'customer_order_interval.csv')
        save_to_csv(order_interval_df, output_path)
