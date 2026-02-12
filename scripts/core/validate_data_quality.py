"""
Script pour valider la qualité des données avant ingestion en base de données
"""
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import logging
from typing import Dict, List, Tuple, Any

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataValidator:
    """
    Classe pour valider la qualité des données avant ingestion en base de données
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialise le validateur avec la configuration
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_data_path = Path(self.config['paths']['processed_data'])
        self.validation_results = {}
        
    def validate_table_integrity(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Valide l'intégrité des données d'une table
        """
        logger.info(f"Validation de l'intégrité pour la table: {table_name}")
        
        integrity_checks = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'data_types': df.dtypes.to_dict()
        }
        
        # Vérifier les valeurs aberrantes dans les colonnes numériques
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        outliers = {}
        
        for col in numeric_columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_count = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            if outlier_count > 0:
                outliers[col] = int(outlier_count)
        
        integrity_checks['outliers'] = outliers
        
        return integrity_checks
    
    def validate_relationships(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Valide les relations entre tables
        """
        logger.info("Validation des relations entre tables")
        
        relationship_checks = {}
        
        # Vérifier les relations possibles entre les tables
        if 'customers_with_geolocation' in tables and 'zip_code_reference' in tables:
            customer_df = tables['customers_with_geolocation']
            zip_ref_df = tables['zip_code_reference']
            
            # Vérifier si tous les codes postaux des clients existent dans la table de référence
            missing_zip_codes = set(customer_df['customer_zip_code_prefix'].unique()) - \
                               set(zip_ref_df['zip_code_prefix'].unique())
            relationship_checks['customer_zip_codes_missing_in_ref'] = list(missing_zip_codes)
            
            # Vérifier si toutes les combinaisons état/ville des clients existent dans la table de référence
            customer_locations = set(zip(
                customer_df['customer_zip_code_prefix'], 
                customer_df['customer_state']
            ))
            zip_ref_locations = set(zip(
                zip_ref_df['zip_code_prefix'], 
                zip_ref_df['state']
            ))
            missing_location_refs = customer_locations - zip_ref_locations
            relationship_checks['customer_location_refs_missing'] = len(missing_location_refs)
        
        if 'sellers_with_geolocation' in tables and 'zip_code_reference' in tables:
            seller_df = tables['sellers_with_geolocation']
            zip_ref_df = tables['zip_code_reference']
            
            # Vérifier si tous les codes postaux des vendeurs existent dans la table de référence
            missing_zip_codes = set(seller_df['seller_zip_code_prefix'].unique()) - \
                               set(zip_ref_df['zip_code_prefix'].unique())
            relationship_checks['seller_zip_codes_missing_in_ref'] = list(missing_zip_codes)
            
            # Vérifier si toutes les combinaisons état/ville des vendeurs existent dans la table de référence
            seller_locations = set(zip(
                seller_df['seller_zip_code_prefix'], 
                seller_df['seller_state']
            ))
            zip_ref_locations = set(zip(
                zip_ref_df['zip_code_prefix'], 
                zip_ref_df['state']
            ))
            missing_location_refs = seller_locations - zip_ref_locations
            relationship_checks['seller_location_refs_missing'] = len(missing_location_refs)
        
        return relationship_checks
    
    def validate_data_types(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Valide les types de données attendus
        """
        logger.info(f"Validation des types de données pour la table: {table_name}")
        
        type_checks = {}
        
        for col, dtype in df.dtypes.items():
            # Vérifier si les types correspondent aux attentes
            if 'id' in col.lower():
                # Les colonnes ID devraient être des chaînes ou des entiers
                if not (dtype in [object, 'int64', 'int32']):
                    type_checks[col] = f"Type inattendu pour une colonne ID: {dtype}"
            elif 'price' in col.lower() or 'amount' in col.lower() or 'total' in col.lower():
                # Les colonnes de prix/devise devraient être numériques
                if not pd.api.types.is_numeric_dtype(dtype):
                    type_checks[col] = f"Colonne prix/devise avec type inattendu: {dtype}"
            elif 'date' in col.lower() or 'time' in col.lower():
                # Les colonnes de date devraient être des dates
                if not pd.api.types.is_datetime64_any_dtype(dtype):
                    type_checks[col] = f"Colonne date avec type inattendu: {dtype}"
        
        return type_checks
    
    def validate_constraints(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Valide les contraintes potentielles sur les données
        """
        logger.info(f"Validation des contraintes pour la table: {table_name}")
        
        constraint_checks = {}
        
        # Vérifier les valeurs négatives là où elles ne devraient pas exister
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if 'price' in col.lower() or 'amount' in col.lower() or 'total' in col.lower() or 'count' in col.lower():
                negative_values = (df[col] < 0).sum()
                if negative_values > 0:
                    constraint_checks[f'{col}_negative_values'] = int(negative_values)
        
        # Vérifier les valeurs nulles là où elles ne devraient pas exister
        id_cols = [col for col in df.columns if 'id' in col.lower()]
        for col in id_cols:
            null_values = df[col].isnull().sum()
            if null_values > 0:
                constraint_checks[f'{col}_null_values'] = int(null_values)
        
        # Vérifier les longueurs de chaînes là où il pourrait y avoir des limites
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            max_length = df[col].astype(str).str.len().max()
            if max_length > 255:  # Arbitrairement choisi comme seuil
                constraint_checks[f'{col}_long_strings'] = {
                    'max_length': int(max_length),
                    'count_longer_than_255': int((df[col].astype(str).str.len() > 255).sum())
                }
        
        return constraint_checks
    
    def load_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Charge toutes les tables de données traitées
        """
        logger.info("Chargement des tables de données traitées")
        
        tables = {}
        
        # Lister les fichiers CSV dans le répertoire de données traitées
        csv_files = list(self.processed_data_path.glob("*.csv"))
        
        # Ajouter les sous-répertoires
        for subdir in self.processed_data_path.iterdir():
            if subdir.is_dir():
                csv_files.extend(subdir.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                # Extraire le nom de la table à partir du nom de fichier
                table_name = csv_file.stem.replace('.csv', '')
                
                # Charger la table
                df = pd.read_csv(csv_file, low_memory=False)
                tables[table_name] = df
                
                logger.info(f"Table chargée: {table_name} ({len(df)} lignes, {len(df.columns)} colonnes)")
            except Exception as e:
                logger.error(f"Erreur lors du chargement de {csv_file}: {str(e)}")
        
        return tables
    
    def validate_for_db_ingestion(self) -> Dict[str, Any]:
        """
        Fonction principale pour valider que les données sont prêtes pour la base de données
        """
        logger.info("Démarrage de la validation complète des données pour ingestion en base")
        
        # Charger toutes les tables
        tables = self.load_tables()
        
        if not tables:
            logger.warning("Aucune table de données trouvée à valider")
            return {"error": "Aucune table de données trouvée"}
        
        # Effectuer toutes les validations
        validation_summary = {
            "validation_timestamp": pd.Timestamp.now().isoformat(),
            "total_tables": len(tables),
            "tables_validated": [],
            "integrity_issues": {},
            "relationship_issues": {},
            "type_issues": {},
            "constraint_issues": {},
            "overall_status": "PASS"
        }
        
        # Valider chaque table individuellement
        for table_name, df in tables.items():
            logger.info(f"Validation de la table: {table_name}")
            
            # Validation de l'intégrité
            integrity_result = self.validate_table_integrity(df, table_name)
            validation_summary["integrity_issues"][table_name] = integrity_result
            
            # Validation des types de données
            type_result = self.validate_data_types(df, table_name)
            if type_result:
                validation_summary["type_issues"][table_name] = type_result
                validation_summary["overall_status"] = "ISSUES_FOUND"
            
            # Validation des contraintes
            constraint_result = self.validate_constraints(df, table_name)
            if constraint_result:
                validation_summary["constraint_issues"][table_name] = constraint_result
                validation_summary["overall_status"] = "ISSUES_FOUND"
            
            validation_summary["tables_validated"].append(table_name)
        
        # Validation des relations entre tables
        relationship_result = self.validate_relationships(tables)
        if relationship_result:
            validation_summary["relationship_issues"] = relationship_result
            validation_summary["overall_status"] = "ISSUES_FOUND"
        
        # Résumé des problèmes trouvés
        issue_counts = {
            "integrity_issues_count": sum(1 for issues in validation_summary["integrity_issues"].values() 
                                        if issues.get('missing_values', {}).values() or 
                                           issues.get('duplicate_rows', 0) > 0 or 
                                           issues.get('outliers', {})),
            "relationship_issues_count": len(validation_summary["relationship_issues"]),
            "type_issues_count": sum(len(issues) for issues in validation_summary["type_issues"].values()),
            "constraint_issues_count": sum(len(issues) for issues in validation_summary["constraint_issues"].values())
        }
        
        validation_summary["issue_counts"] = issue_counts
        
        logger.info(f"Validation terminée. Statut global: {validation_summary['overall_status']}")
        
        return validation_summary
    
    def generate_validation_report(self, validation_results: Dict[str, Any], output_path: str = None):
        """
        Génère un rapport de validation
        """
        if output_path is None:
            output_path = self.processed_data_path / "validation_report.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("RAPPORT DE VALIDATION DES DONNÉES POUR INGESTION EN BASE\n")
            f.write("="*70 + "\n")
            f.write(f"Date de validation: {validation_results['validation_timestamp']}\n")
            f.write(f"Statut global: {validation_results['overall_status']}\n")
            f.write(f"Nombre total de tables: {validation_results['total_tables']}\n")
            f.write(f"Tables validées: {', '.join(validation_results['tables_validated'])}\n\n")
            
            f.write("RÉSUMÉ DES PROBLÈMES TROUVÉS\n")
            f.write("-"*40 + "\n")
            for issue_type, count in validation_results['issue_counts'].items():
                f.write(f"{issue_type}: {count}\n")
            
            if validation_results['type_issues']:
                f.write("\nPROBLÈMES DE TYPES DE DONNÉES\n")
                f.write("-"*40 + "\n")
                for table, issues in validation_results['type_issues'].items():
                    f.write(f"\nTable '{table}':\n")
                    for col, issue in issues.items():
                        f.write(f"  - {col}: {issue}\n")
            
            if validation_results['constraint_issues']:
                f.write("\nVIOLATIONS DE CONTRAINTES\n")
                f.write("-"*40 + "\n")
                for table, issues in validation_results['constraint_issues'].items():
                    f.write(f"\nTable '{table}':\n")
                    for constraint, details in issues.items():
                        f.write(f"  - {constraint}: {details}\n")
            
            if validation_results['relationship_issues']:
                f.write("\nPROBLÈMES DE RELATIONS ENTRE TABLES\n")
                f.write("-"*40 + "\n")
                for issue, details in validation_results['relationship_issues'].items():
                    f.write(f"{issue}: {details}\n")
        
        logger.info(f"Rapport de validation généré: {output_path}")


def validate_for_db_ingestion():
    """
    Fonction pour valider que les données sont prêtes pour la base de données
    """
    validator = DataValidator()
    results = validator.validate_for_db_ingestion()
    
    # Afficher un résumé dans la console
    print("="*70)
    print("VALIDATION DES DONNÉES POUR INGESTION EN BASE DE DONNÉES")
    print("="*70)
    print(f"Statut global: {results['overall_status']}")
    print(f"Tables validées: {results['total_tables']}")
    print(f"Problèmes trouvés: {sum(results['issue_counts'].values())}")
    
    for issue_type, count in results['issue_counts'].items():
        print(f"- {issue_type}: {count}")
    
    # Générer le rapport détaillé
    validator.generate_validation_report(results)
    
    return results


if __name__ == "__main__":
    validate_for_db_ingestion()