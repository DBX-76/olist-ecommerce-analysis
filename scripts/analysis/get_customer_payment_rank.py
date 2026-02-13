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

def get_customer_payment_rank():
    """
    Récupère le classement de chaque client en fonction du montant total de ses paiements.
    
    Returns:
        pandas.DataFrame: DataFrame contenant le customer_unique_id, le montant total payé et le rang
    """
    try:
        db_url = get_database_url()
        engine = create_engine(db_url)
        
        # Requête SQL avec CTE et RANK()
        query = text("""
            WITH customer_total_payments AS (
                SELECT 
                    c.customer_unique_id,
                    SUM(p.payment_value) AS total_payment_amount
                FROM 
                    payments p
                JOIN 
                    orders o ON p.order_id = o.order_id
                JOIN 
                    customers c ON o.customer_id = c.customer_id
                GROUP BY 
                    c.customer_unique_id
            )
            SELECT 
                customer_unique_id,
                total_payment_amount,
                RANK() OVER (ORDER BY total_payment_amount DESC) AS payment_rank
            FROM 
                customer_total_payments
            ORDER BY 
                payment_rank;
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        print(f"Classement des clients récupéré avec succès!")
        print(f"Nombre de clients uniques: {len(df)}")
        print(f"Montant total des paiements: {df['total_payment_amount'].sum():,.2f}")
        print(f"Montant moyen par client: {df['total_payment_amount'].mean():,.2f}")
        
        return df
        
    except Exception as e:
        print(f"Erreur lors de la récupération du classement des clients: {e}")
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
        print(f"Classement des clients sauvegardé avec succès dans {output_path}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du fichier: {e}")

if __name__ == "__main__":
    # Récupérer le classement des clients
    customer_rank_df = get_customer_payment_rank()
    
    # Sauvegarder dans un fichier CSV
    if customer_rank_df is not None:
        output_dir = 'reports'
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, 'customer_payment_rank.csv')
        save_to_csv(customer_rank_df, output_path)
