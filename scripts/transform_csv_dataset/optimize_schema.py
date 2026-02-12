"""
Script pour analyser et optimiser le schéma de données (normalisation)
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

class SchemaAnalyzer:
    """
    Classe pour analyser et optimiser le schéma de données
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialise l'analyseur avec la configuration
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_data_path = Path(self.config['paths']['processed_data'])
        
    def analyze_normalization_levels(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Analyse les niveaux de normalisation d'une table
        """
        logger.info(f"Analyse de la normalisation pour la table: {table_name}")
        
        analysis = {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'potential_anomalies': [],
            'normalization_recommendations': []
        }
        
        # Identifier les colonnes qui pourraient être dénormalisées
        # Chercher des colonnes avec des valeurs répétitives
        repetitive_columns = []
        for col in df.columns:
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.1:  # Moins de 10% de valeurs uniques
                repetitive_columns.append({
                    'column': col,
                    'unique_count': df[col].nunique(),
                    'unique_ratio': unique_ratio,
                    'top_values': df[col].value_counts().head(5).to_dict()
                })
        
        analysis['repetitive_columns'] = repetitive_columns
        
        # Identifier les colonnes avec des dépendances fonctionnelles
        # Par exemple, si une colonne peut être déterminée à partir d'une autre
        functional_dependencies = []
        columns = df.columns.tolist()
        
        for i, col1 in enumerate(columns):
            for col2 in columns[i+1:]:
                # Vérifier si col2 dépend fonctionnellement de col1
                grouped = df.groupby(col1)[col2].nunique()
                if (grouped == 1).all():  # Pour chaque valeur de col1, il n'y a qu'une seule valeur de col2
                    functional_dependencies.append({
                        'dependent_column': col2,
                        'determinant_column': col1,
                        'dependency_ratio': 1.0
                    })
        
        analysis['functional_dependencies'] = functional_dependencies
        
        # Identifier les colonnes qui pourraient être divisées (ex: nom complet en prénom/nom)
        potential_splits = []
        for col in df.select_dtypes(include=['object']).columns:
            # Vérifier si la colonne contient des séparateurs courants
            if df[col].dropna().apply(lambda x: isinstance(x, str) and (' ' in x or '-' in x or '_' in x)).any():
                avg_word_count = df[col].dropna().apply(lambda x: len(str(x).split()) if isinstance(x, str) else 0).mean()
                if avg_word_count > 1.5:
                    potential_splits.append({
                        'column': col,
                        'average_word_count': avg_word_count
                    })
        
        analysis['potential_splits'] = potential_splits
        
        return analysis
    
    def analyze_data_types_optimization(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Analyse les opportunités d'optimisation des types de données
        """
        logger.info(f"Analyse d'optimisation des types de données pour la table: {table_name}")
        
        type_analysis = {
            'table_name': table_name,
            'current_memory_usage': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'optimization_opportunities': []
        }
        
        for col in df.columns:
            original_dtype = df[col].dtype
            original_memory = df[col].memory_usage(deep=True) / len(df) if len(df) > 0 else 0
            
            optimization = {'column': col, 'original_type': str(original_dtype)}
            
            if pd.api.types.is_integer_dtype(original_dtype):
                # Trouver la plage de valeurs pour optimiser le type entier
                min_val = df[col].min()
                max_val = df[col].max()
                
                if pd.notna(min_val) and pd.notna(max_val):
                    if min_val >= 0:
                        if max_val <= 255:
                            new_type = 'uint8'
                        elif max_val <= 65535:
                            new_type = 'uint16'
                        elif max_val <= 4294967295:
                            new_type = 'uint32'
                        else:
                            new_type = 'uint64'
                    else:
                        if min_val >= -128 and max_val <= 127:
                            new_type = 'int8'
                        elif min_val >= -32768 and max_val <= 32767:
                            new_type = 'int16'
                        elif min_val >= -2147483648 and max_val <= 2147483647:
                            new_type = 'int32'
                        else:
                            new_type = 'int64'
                    
                    if new_type != original_dtype.name:
                        optimization['optimized_type'] = new_type
                        optimization['memory_reduction_potential'] = f"Potentiel de réduction de mémoire"
            
            elif pd.api.types.is_float_dtype(original_dtype):
                # Vérifier si float32 suffit au lieu de float64
                if original_dtype.name == 'float64':
                    # Vérifier la précision nécessaire
                    if df[col].apply(lambda x: pd.isna(x) or x == np.float32(x)).all():
                        optimization['optimized_type'] = 'float32'
                        optimization['memory_reduction_potential'] = f"Réduction potentielle de 50% de mémoire"
            
            elif pd.api.types.is_object_dtype(original_dtype):
                # Vérifier si c'est une chaîne de caractères et si category serait plus efficace
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:  # Si moins de 50% de valeurs uniques
                    optimization['category_option'] = True
                    optimization['unique_values_ratio'] = round(unique_ratio, 3)
            
            if 'optimized_type' in optimization or 'category_option' in optimization:
                type_analysis['optimization_opportunities'].append(optimization)
        
        return type_analysis
    
    def suggest_denormalization_opportunities(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Suggère des opportunités de dénormalisation pour améliorer les performances
        """
        logger.info("Analyse des opportunités de dénormalisation")
        
        denormalization_suggestions = []
        
        # Exemple: si une table est fréquemment jointe avec une autre petite table
        # on pourrait envisager d'inclure certaines colonnes pour éviter la jointure
        
        # Vérifier les relations entre les tables existantes
        if 'customers_with_geolocation' in tables and 'zip_code_reference' in tables:
            customer_df = tables['customers_with_geolocation']
            zip_ref_df = tables['zip_code_reference']
            
            # Calculer la fréquence d'utilisation des jointures potentielles
            customer_zip_count = customer_df['customer_zip_code_prefix'].value_counts()
            zip_ref_count = zip_ref_df['zip_code_prefix'].value_counts()
            
            # Suggérer d'ajouter certaines colonnes fréquentes de zip_code_reference à customers
            common_zip_codes = customer_zip_count.head(10).index
            common_zip_data = zip_ref_df[zip_ref_df['zip_code_prefix'].isin(common_zip_codes)]
            
            if len(common_zip_data) > 0:
                denormalization_suggestions.append({
                    'from_table': 'zip_code_reference',
                    'to_table': 'customers_with_geolocation',
                    'columns_to_add': ['canonical_city_name', 'avg_latitude', 'avg_longitude'],
                    'rationale': 'Éviter les jointures fréquentes pour les consultations géographiques'
                })
        
        if 'sellers_with_geolocation' in tables and 'zip_code_reference' in tables:
            seller_df = tables['sellers_with_geolocation']
            zip_ref_df = tables['zip_code_reference']
            
            seller_zip_count = seller_df['seller_zip_code_prefix'].value_counts()
            common_seller_zips = seller_zip_count.head(10).index
            common_seller_zip_data = zip_ref_df[zip_ref_df['zip_code_prefix'].isin(common_seller_zips)]
            
            if len(common_seller_zip_data) > 0:
                denormalization_suggestions.append({
                    'from_table': 'zip_code_reference',
                    'to_table': 'sellers_with_geolocation',
                    'columns_to_add': ['canonical_city_name', 'avg_latitude', 'avg_longitude'],
                    'rationale': 'Éviter les jointures fréquentes pour les consultations géographiques'
                })
        
        return {
            'denormalization_suggestions': denormalization_suggestions,
            'considerations': [
                'La dénormalisation peut améliorer les performances de lecture',
                'Mais elle augmente la redondance et la complexité de mise à jour',
                'À utiliser judicieusement selon les cas d\'utilisation'
            ]
        }
    
    def analyze_schema_optimization(self) -> Dict[str, Any]:
        """
        Analyse complète de l'optimisation du schéma
        """
        logger.info("Démarrage de l'analyse d'optimisation du schéma de données")
        
        # Charger toutes les tables
        tables = self.load_tables()
        
        if not tables:
            logger.warning("Aucune table de données trouvée à analyser")
            return {"error": "Aucune table de données trouvée"}
        
        analysis_results = {
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
            "total_tables_analyzed": len(tables),
            "normalization_analysis": {},
            "data_type_optimization": {},
            "denormalization_opportunities": {},
            "recommendations": []
        }
        
        # Analyser chaque table
        for table_name, df in tables.items():
            logger.info(f"Analyse de la table: {table_name}")
            
            # Analyse de normalisation
            normalization_result = self.analyze_normalization_levels(df, table_name)
            analysis_results["normalization_analysis"][table_name] = normalization_result
            
            # Analyse d'optimisation des types de données
            type_optimization_result = self.analyze_data_types_optimization(df, table_name)
            analysis_results["data_type_optimization"][table_name] = type_optimization_result
        
        # Analyse des opportunités de dénormalisation
        denormalization_result = self.suggest_denormalization_opportunities(tables)
        analysis_results["denormalization_opportunities"] = denormalization_result
        
        # Générer des recommandations générales
        recommendations = self.generate_general_recommendations(analysis_results)
        analysis_results["recommendations"] = recommendations
        
        logger.info("Analyse d'optimisation du schéma terminée")
        
        return analysis_results
    
    def load_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Charge toutes les tables de données traitées
        """
        logger.info("Chargement des tables de données pour l'analyse de schéma")
        
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
    
    def generate_general_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Génère des recommandations générales basées sur l'analyse
        """
        recommendations = []
        
        # Recommandations basées sur les analyses de normalisation
        for table_name, norm_analysis in analysis_results["normalization_analysis"].items():
            if norm_analysis.get('repetitive_columns'):
                recommendations.append(
                    f"Pour la table '{table_name}', envisager de créer une table de dimension "
                    f"pour les colonnes répétitives: {[col['column'] for col in norm_analysis['repetitive_columns']]}"
                )
        
        # Recommandations basées sur les analyses de types de données
        for table_name, type_analysis in analysis_results["data_type_optimization"].items():
            if type_analysis.get('optimization_opportunities'):
                optimizations = type_analysis['optimization_opportunities']
                columns_to_optimize = [opt['column'] for opt in optimizations]
                recommendations.append(
                    f"Pour la table '{table_name}', optimiser les types de données pour les colonnes: {columns_to_optimize}"
                )
        
        # Recommandations basées sur les opportunités de dénormalisation
        denorm_suggestions = analysis_results["denormalization_opportunities"]["denormalization_suggestions"]
        for suggestion in denorm_suggestions:
            recommendations.append(
                f"Dénormalisation suggérée: Ajouter les colonnes {suggestion['columns_to_add']} "
                f"de '{suggestion['from_table']}' à '{suggestion['to_table']}' pour {suggestion['rationale']}"
            )
        
        return recommendations
    
    def generate_schema_optimization_report(self, analysis_results: Dict[str, Any], output_path: str = None):
        """
        Génère un rapport d'analyse d'optimisation du schéma
        """
        if output_path is None:
            output_path = self.processed_data_path / "schema_optimization_report.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("RAPPORT D'ANALYSE D'OPTIMISATION DU SCHÉMA DE DONNÉES\n")
            f.write("="*70 + "\n")
            f.write(f"Date d'analyse: {analysis_results['analysis_timestamp']}\n")
            f.write(f"Nombre total de tables analysées: {analysis_results['total_tables_analyzed']}\n\n")
            
            f.write("ANALYSE DE NORMALISATION\n")
            f.write("-"*40 + "\n")
            for table_name, norm_analysis in analysis_results['normalization_analysis'].items():
                f.write(f"\nTable '{table_name}':\n")
                f.write(f"  - Lignes: {norm_analysis['row_count']}, Colonnes: {norm_analysis['column_count']}\n")
                
                if norm_analysis['repetitive_columns']:
                    f.write("  - Colonnes répétitives (potentiellement candidates à la décomposition):\n")
                    for col_info in norm_analysis['repetitive_columns']:
                        f.write(f"    * {col_info['column']}: {col_info['unique_count']} valeurs uniques "
                                f"({col_info['unique_ratio']:.2%})\n")
                
                if norm_analysis['functional_dependencies']:
                    f.write("  - Dépendances fonctionnelles détectées:\n")
                    for dep in norm_analysis['functional_dependencies']:
                        f.write(f"    * {dep['dependent_column']} dépend de {dep['determinant_column']}\n")
            
            f.write("\n\nOPTIMISATION DES TYPES DE DONNÉES\n")
            f.write("-"*40 + "\n")
            for table_name, type_analysis in analysis_results['data_type_optimization'].items():
                f.write(f"\nTable '{table_name}' (utilisation mémoire: {type_analysis['current_memory_usage']:.2f} MB):\n")
                
                if type_analysis['optimization_opportunities']:
                    f.write("  - Opportunités d'optimisation:\n")
                    for opt in type_analysis['optimization_opportunities']:
                        f.write(f"    * Colonne '{opt['column']}' (type original: {opt['original_type']})\n")
                        if 'optimized_type' in opt:
                            f.write(f"      → Type optimisé suggéré: {opt['optimized_type']}\n")
                        if 'category_option' in opt:
                            f.write(f"      → Option catégorie suggérée (ratio uniques: {opt.get('unique_values_ratio', 0)})\n")
            
            f.write("\n\nOPPORTUNITÉS DE DÉNORMALISATION\n")
            f.write("-"*40 + "\n")
            denorm_suggestions = analysis_results['denormalization_opportunities']['denormalization_suggestions']
            if denorm_suggestions:
                for suggestion in denorm_suggestions:
                    f.write(f"\n- De '{suggestion['from_table']}' vers '{suggestion['to_table']}':\n")
                    f.write(f"  Colonnes à ajouter: {suggestion['columns_to_add']}\n")
                    f.write(f"  Raison: {suggestion['rationale']}\n")
            else:
                f.write("Aucune opportunité de dénormalisation significative détectée.\n")
            
            f.write("\n\nRECOMMANDATIONS GÉNÉRALES\n")
            f.write("-"*40 + "\n")
            for rec in analysis_results['recommendations']:
                f.write(f"- {rec}\n")
        
        logger.info(f"Rapport d'optimisation de schéma généré: {output_path}")


def analyze_and_optimize_schema():
    """
    Fonction principale pour analyser et optimiser le schéma de données
    """
    analyzer = SchemaAnalyzer()
    results = analyzer.analyze_schema_optimization()
    
    # Afficher un résumé dans la console
    print("="*70)
    print("ANALYSE D'OPTIMISATION DU SCHÉMA DE DONNÉES")
    print("="*70)
    print(f"Tables analysées: {results['total_tables_analyzed']}")
    print(f"Recommandations générées: {len(results['recommendations'])}")
    
    print("\nRésumé des recommandations:")
    for rec in results['recommendations'][:5]:  # Afficher les 5 premières
        print(f"- {rec}")
    if len(results['recommendations']) > 5:
        print(f"... et {len(results['recommendations']) - 5} autres")
    
    # Générer le rapport détaillé
    analyzer.generate_schema_optimization_report(results)
    
    return results


if __name__ == "__main__":
    analyze_and_optimize_schema()