"""
Script pour identifier les colonnes nécessitant des index et définir les contraintes d'intégrité
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

class IndexAndConstraintAnalyzer:
    """
    Classe pour analyser les besoins en index et les contraintes d'intégrité
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialise l'analyseur avec la configuration
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_data_path = Path(self.config['paths']['processed_data'])
        
    def analyze_index_needs(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Analyse les besoins en index pour une table
        """
        logger.info(f"Analyse des besoins en index pour la table: {table_name}")
        
        index_analysis = {
            'table_name': table_name,
            'candidate_indexes': [],
            'query_patterns': []
        }
        
        # Identifier les colonnes candidates pour les index
        # Basé sur la cardinalité, l'utilisation dans les filtres, etc.
        
        for col in df.columns:
            col_info = {
                'column': col,
                'data_type': str(df[col].dtype),
                'cardinality': df[col].nunique(),
                'total_rows': len(df),
                'cardinality_ratio': df[col].nunique() / len(df) if len(df) > 0 else 0
            }
            
            # Déterminer si la colonne est candidate à un index
            is_candidate = False
            reasons = []
            
            # Les colonnes ID sont souvent candidates à un index
            if 'id' in col.lower():
                is_candidate = True
                reasons.append('Colonne ID')
            
            # Les colonnes avec haute cardinalité peuvent bénéficier d'index
            if col_info['cardinality_ratio'] > 0.01:  # Plus de 1% de valeurs uniques
                is_candidate = True
                reasons.append('Haute cardinalité')
            
            # Les colonnes fréquemment utilisées dans les jointures
            if any(join_col in col.lower() for join_col in ['zip_code', 'state', 'city']):
                is_candidate = True
                reasons.append('Utilisée dans les jointures géographiques')
            
            # Les colonnes utilisées dans les filtres courants
            if any(filter_col in col.lower() for filter_col in ['date', 'status', 'category']):
                is_candidate = True
                reasons.append('Potentiellement utilisée dans les filtres')
            
            if is_candidate:
                col_info['is_index_candidate'] = True
                col_info['reasons'] = reasons
                
                # Déterminer le type d'index approprié
                if col_info['cardinality_ratio'] == 1.0 and 'id' in col.lower():
                    col_info['recommended_index_type'] = 'PRIMARY KEY'
                elif col_info['cardinality_ratio'] == 1.0:
                    col_info['recommended_index_type'] = 'UNIQUE'
                elif col_info['cardinality_ratio'] > 0.1:
                    col_info['recommended_index_type'] = 'INDEX'
                else:
                    col_info['recommended_index_type'] = 'INDEX (potentiellement composite)'
                
                index_analysis['candidate_indexes'].append(col_info)
        
        # Identifier les index composites potentiels
        # Par exemple, pour les requêtes de date et statut ensemble
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        status_cols = [col for col in df.columns if 'status' in col.lower()]
        
        if date_cols and status_cols:
            for date_col in date_cols:
                for status_col in status_cols:
                    composite_index = {
                        'columns': [date_col, status_col],
                        'type': 'COMPOSITE INDEX',
                        'use_case': f'Filtrage par {date_col} et {status_col}'
                    }
                    index_analysis['candidate_indexes'].append(composite_index)
        
        return index_analysis
    
    def analyze_primary_keys(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Analyse les candidats potentiels pour les clés primaires
        """
        logger.info(f"Analyse des clés primaires pour la table: {table_name}")
        
        pk_analysis = {
            'table_name': table_name,
            'potential_primary_keys': [],
            'single_column_pks': [],
            'composite_pks': []
        }
        
        # Chercher des colonnes qui pourraient être des clés primaires
        for col in df.columns:
            if df[col].is_unique and df[col].notna().all():
                pk_analysis['single_column_pks'].append({
                    'column': col,
                    'type': str(df[col].dtype),
                    'is_suitable_pk': True
                })
        
        # Chercher des combinaisons de colonnes qui pourraient former des clés primaires
        # Essayons les paires de colonnes courantes
        id_cols = [col for col in df.columns if 'id' in col.lower()]
        
        for i, col1 in enumerate(id_cols):
            for col2 in id_cols[i+1:]:
                if df[[col1, col2]].duplicated().sum() == 0:
                    pk_analysis['composite_pks'].append({
                        'columns': [col1, col2],
                        'is_suitable_pk': True
                    })
        
        # Pour les tables de faits, on pourrait avoir des clés composites
        # incluant des clés étrangères et des dates
        if 'order' in table_name.lower() or 'item' in table_name.lower() or 'payment' in table_name.lower():
            # Ces tables pourraient avoir des clés composites
            potential_fk_cols = [col for col in df.columns if any(fk in col.lower() for fk in ['id', 'key'])]
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            
            if potential_fk_cols and date_cols:
                pk_analysis['composite_pks'].append({
                    'columns': potential_fk_cols[:2] + date_cols[:1],  # Limiter à 3 colonnes max
                    'is_suitable_pk': True,
                    'for_fact_table': True
                })
        
        pk_analysis['potential_primary_keys'] = pk_analysis['single_column_pks'] + pk_analysis['composite_pks']
        
        return pk_analysis
    
    def analyze_foreign_keys(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Analyse les relations entre tables pour identifier les clés étrangères
        """
        logger.info("Analyse des relations de clés étrangères entre tables")
        
        fk_analysis = {
            'foreign_key_candidates': [],
            'referential_integrity_issues': []
        }
        
        table_names = list(tables.keys())
        
        # Analyser les relations possibles entre les tables
        for i, table1_name in enumerate(table_names):
            table1 = tables[table1_name]
            
            for table2_name in table_names[i+1:]:
                table2 = tables[table2_name]
                
                # Chercher des colonnes avec des noms similaires qui pourraient indiquer une relation
                for col1 in table1.columns:
                    for col2 in table2.columns:
                        # Vérifier si les colonnes ont des noms similaires et des types compatibles
                        if (col1.lower().replace('_', '') == col2.lower().replace('_', '') and 
                            col1 != col2 and
                            pd.api.types.is_dtype_equal(table1[col1].dtype, table2[col2].dtype)):
                            
                            # Vérifier si toutes les valeurs de table1[col1] existent dans table2[col2]
                            missing_values = set(table1[col1].dropna().unique()) - set(table2[col2].dropna().unique())
                            
                            fk_candidate = {
                                'parent_table': table2_name,
                                'child_table': table1_name,
                                'parent_column': col2,
                                'child_column': col1,
                                'missing_references': len(missing_values),
                                'missing_sample': list(missing_values)[:5] if missing_values else []
                            }
                            
                            fk_analysis['foreign_key_candidates'].append(fk_candidate)
        
        # Analyser les relations spécifiques basées sur le schéma connu
        # (basé sur les noms de colonnes et le contexte métier)
        specific_relations = [
            # Relations clientes-adresses
            {
                'parent_table': 'zip_code_reference',
                'child_table': 'customers_with_geolocation',
                'parent_column': 'zip_code_prefix',
                'child_column': 'customer_zip_code_prefix'
            },
            {
                'parent_table': 'zip_code_reference',
                'child_table': 'sellers_with_geolocation',
                'parent_column': 'zip_code_prefix',
                'child_column': 'seller_zip_code_prefix'
            },
            # Relation commandes-paiements
            {
                'parent_table': 'order_financial_reconciliation',
                'child_table': 'order_financial_reconciliation',  # Cette table contient déjà les relations
                'parent_column': 'order_id',
                'child_column': 'order_id'
            }
        ]
        
        for rel in specific_relations:
            parent_table = rel['parent_table']
            child_table = rel['child_table']
            parent_col = rel['parent_column']
            child_col = rel['child_column']
            
            if parent_table in tables and child_table in tables:
                parent_df = tables[parent_table]
                child_df = tables[child_table]
                
                if parent_col in parent_df.columns and child_col in child_df.columns:
                    # Vérifier l'intégrité référentielle
                    missing_refs = set(child_df[child_col].dropna().unique()) - set(parent_df[parent_col].dropna().unique())
                    
                    if missing_refs:
                        fk_analysis['referential_integrity_issues'].append({
                            'relation': f"{child_table}.{child_col} -> {parent_table}.{parent_col}",
                            'missing_references_count': len(missing_refs),
                            'sample_missing_values': list(missing_refs)[:10]
                        })
        
        return fk_analysis
    
    def define_constraints(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Définit les contraintes d'intégrité pour une table
        """
        logger.info(f"Définition des contraintes pour la table: {table_name}")
        
        constraints = {
            'table_name': table_name,
            'primary_key_constraints': [],
            'foreign_key_constraints': [],
            'check_constraints': [],
            'not_null_constraints': [],
            'unique_constraints': []
        }
        
        # Contraintes NOT NULL pour les colonnes ID
        for col in df.columns:
            if 'id' in col.lower() and df[col].isna().sum() == 0:
                constraints['not_null_constraints'].append({
                    'column': col,
                    'constraint_type': 'NOT NULL',
                    'reason': 'Les colonnes ID ne devraient pas être nulles'
                })
        
        # Contraintes CHECK pour les colonnes numériques
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if 'price' in col.lower() or 'amount' in col.lower() or 'freight' in col.lower():
                # Les prix/montants ne devraient pas être négatifs
                negative_count = (df[col] < 0).sum()
                if negative_count == 0:
                    constraints['check_constraints'].append({
                        'column': col,
                        'constraint_type': 'CHECK',
                        'condition': f'{col} >= 0',
                        'reason': 'Les montants ne devraient pas être négatifs'
                    })
            elif 'quantity' in col.lower() or 'count' in col.lower():
                # Les quantités/comptes ne devraient pas être négatifs
                negative_count = (df[col] < 0).sum()
                if negative_count == 0:
                    constraints['check_constraints'].append({
                        'column': col,
                        'constraint_type': 'CHECK',
                        'condition': f'{col} >= 0',
                        'reason': 'Les quantités ne devraient pas être négatives'
                    })
        
        # Contraintes UNIQUE pour les colonnes ID uniques
        for col in df.columns:
            if df[col].is_unique and 'id' in col.lower():
                constraints['unique_constraints'].append({
                    'column': col,
                    'constraint_type': 'UNIQUE',
                    'reason': 'Les colonnes ID devraient être uniques'
                })
        
        return constraints
    
    def analyze_indexes_and_constraints(self) -> Dict[str, Any]:
        """
        Analyse complète des besoins en index et contraintes
        """
        logger.info("Démarrage de l'analyse des index et contraintes")
        
        # Charger toutes les tables
        tables = self.load_tables()
        
        if not tables:
            logger.warning("Aucune table de données trouvée à analyser")
            return {"error": "Aucune table de données trouvée"}
        
        analysis_results = {
            "analysis_timestamp": pd.Timestamp.now().isoformat(),
            "total_tables_analyzed": len(tables),
            "index_analysis": {},
            "primary_key_analysis": {},
            "foreign_key_analysis": {},
            "constraint_definitions": {},
            "sql_ddl_statements": []
        }
        
        # Analyser chaque table
        for table_name, df in tables.items():
            logger.info(f"Analyse de la table: {table_name}")
            
            # Analyse des besoins en index
            index_result = self.analyze_index_needs(df, table_name)
            analysis_results["index_analysis"][table_name] = index_result
            
            # Analyse des clés primaires
            pk_result = self.analyze_primary_keys(df, table_name)
            analysis_results["primary_key_analysis"][table_name] = pk_result
            
            # Définition des contraintes
            constraint_result = self.define_constraints(df, table_name)
            analysis_results["constraint_definitions"][table_name] = constraint_result
        
        # Analyse des clés étrangères entre tables
        fk_result = self.analyze_foreign_keys(tables)
        analysis_results["foreign_key_analysis"] = fk_result
        
        # Générer les énoncés DDL SQL
        ddl_statements = self.generate_sql_ddl(tables, analysis_results)
        analysis_results["sql_ddl_statements"] = ddl_statements
        
        logger.info("Analyse des index et contraintes terminée")
        
        return analysis_results
    
    def load_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Charge toutes les tables de données traitées
        """
        logger.info("Chargement des tables de données pour l'analyse d'index et contraintes")
        
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
    
    def generate_sql_ddl(self, tables: Dict[str, pd.DataFrame], analysis_results: Dict[str, Any]) -> List[str]:
        """
        Génère les énoncés SQL DDL pour créer les tables avec index et contraintes
        """
        ddl_statements = []
        
        # Générer les CREATE TABLE pour chaque table
        for table_name, df in tables.items():
            create_statement = f"CREATE TABLE {table_name} (\n"
            
            columns_def = []
            primary_keys = []
            
            for col in df.columns:
                # Déterminer le type SQL approprié
                sql_type = self.map_pandas_to_sql_type(df[col].dtype)
                
                col_def = f"    {col} {sql_type}"
                
                # Ajouter les contraintes NOT NULL si pertinent
                if df[col].isna().sum() == 0 and 'id' in col.lower():
                    col_def += " NOT NULL"
                
                # Si c'est une clé primaire
                pk_analysis = analysis_results["primary_key_analysis"][table_name]
                if any(col == pk.get('column') for pk in pk_analysis.get('single_column_pks', [])):
                    col_def += " PRIMARY KEY"
                    primary_keys.append(col)
                
                columns_def.append(col_def)
            
            # Si on a des clés primaires multiples, on les définit séparément
            if len(primary_keys) > 1:
                # Retirer les PRIMARY KEY des définitions de colonnes
                for i, col_def in enumerate(columns_def):
                    if " PRIMARY KEY" in col_def:
                        columns_def[i] = col_def.replace(" PRIMARY KEY", "")
                
                # Ajouter la contrainte de clé primaire composite
                pk_constraint = f"    CONSTRAINT PK_{table_name} PRIMARY KEY ({', '.join(primary_keys)})"
                columns_def.append(pk_constraint)
            
            create_statement += ",\n".join(columns_def)
            create_statement += "\n);"
            
            ddl_statements.append(create_statement)
        
        # Générer les énoncés CREATE INDEX
        for table_name, index_analysis in analysis_results["index_analysis"].items():
            for candidate in index_analysis["candidate_indexes"]:
                if 'column' in candidate:  # Index simple
                    col = candidate['column']
                    idx_name = f"IDX_{table_name}_{col}"
                    if candidate['recommended_index_type'] in ['UNIQUE', 'PRIMARY KEY']:
                        ddl_statements.append(f"CREATE UNIQUE INDEX {idx_name} ON {table_name} ({col});")
                    else:
                        ddl_statements.append(f"CREATE INDEX {idx_name} ON {table_name} ({col});")
                elif 'columns' in candidate:  # Index composite
                    cols = candidate['columns']
                    idx_name = f"IDX_{table_name}_{'_'.join(cols)}"
                    ddl_statements.append(f"CREATE INDEX {idx_name} ON {table_name} ({', '.join(cols)});")
        
        # Générer les énoncés ALTER TABLE pour les clés étrangères
        for fk_relation in analysis_results["foreign_key_analysis"]["foreign_key_candidates"]:
            parent_table = fk_relation['parent_table']
            child_table = fk_relation['child_table']
            parent_col = fk_relation['parent_column']
            child_col = fk_relation['child_column']
            
            fk_name = f"FK_{child_table}_{child_col}_ref_{parent_table}_{parent_col}"
            ddl_statements.append(
                f"ALTER TABLE {child_table} ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({child_col}) REFERENCES {parent_table}({parent_col});"
            )
        
        return ddl_statements
    
    def map_pandas_to_sql_type(self, pandas_dtype) -> str:
        """
        Convertit un type pandas en type SQL équivalent
        """
        if pd.api.types.is_integer_dtype(pandas_dtype):
            # Déterminer la taille appropriée basée sur les valeurs
            return "INTEGER"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "DECIMAL(15,2)"  # Pour les montants monétaires
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "TIMESTAMP"
        else:
            # Pour les objets/string, déterminer la longueur maximale
            return "VARCHAR(255)"  # Taille par défaut
    
    def generate_index_constraint_report(self, analysis_results: Dict[str, Any], output_path: str = None):
        """
        Génère un rapport d'analyse des index et contraintes
        """
        if output_path is None:
            output_path = self.processed_data_path / "index_constraint_analysis_report.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("RAPPORT D'ANALYSE DES INDEX ET CONTRAINTES D'INTÉGRITÉ\n")
            f.write("="*70 + "\n")
            f.write(f"Date d'analyse: {analysis_results['analysis_timestamp']}\n")
            f.write(f"Nombre total de tables analysées: {analysis_results['total_tables_analyzed']}\n\n")
            
            f.write("ANALYSE DES INDEX\n")
            f.write("-"*40 + "\n")
            for table_name, index_analysis in analysis_results['index_analysis'].items():
                f.write(f"\nTable '{table_name}':\n")
                if index_analysis['candidate_indexes']:
                    f.write("  - Colonnes candidates à l'indexation:\n")
                    for candidate in index_analysis['candidate_indexes']:
                        if 'column' in candidate:
                            f.write(f"    * {candidate['column']} - Type: {candidate['recommended_index_type']}\n")
                            f.write(f"      Raisons: {', '.join(candidate['reasons'])}\n")
                        elif 'columns' in candidate:
                            f.write(f"    * Index composite sur: {candidate['columns']} - Type: {candidate['type']}\n")
                            f.write(f"      Cas d'utilisation: {candidate['use_case']}\n")
                else:
                    f.write("  - Aucun index candidat identifié\n")
            
            f.write("\n\nANALYSE DES CLÉS PRIMAIRES\n")
            f.write("-"*40 + "\n")
            for table_name, pk_analysis in analysis_results['primary_key_analysis'].items():
                f.write(f"\nTable '{table_name}':\n")
                if pk_analysis['potential_primary_keys']:
                    f.write("  - Potentielles clés primaires:\n")
                    for pk in pk_analysis['potential_primary_keys']:
                        if 'column' in pk:
                            f.write(f"    * Colonne unique: {pk['column']} (type: {pk['type']})\n")
                        elif 'columns' in pk:
                            f.write(f"    * Clé composite: {pk['columns']}\n")
                else:
                    f.write("  - Aucune clé primaire potentielle identifiée\n")
            
            f.write("\n\nCLÉS ÉTRANGÈRES ET INTÉGRITÉ RÉFÉRENTIELLE\n")
            f.write("-"*40 + "\n")
            fk_analysis = analysis_results['foreign_key_analysis']
            if fk_analysis['foreign_key_candidates']:
                f.write("  - Relations candidates à des clés étrangères:\n")
                for fk in fk_analysis['foreign_key_candidates']:
                    f.write(f"    * {fk['child_table']}.{fk['child_column']} -> {fk['parent_table']}.{fk['parent_column']}\n")
                    if fk['missing_references'] > 0:
                        f.write(f"      - {fk['missing_references']} références manquantes\n")
            else:
                f.write("  - Aucune relation de clé étrangère identifiée\n")
            
            if fk_analysis['referential_integrity_issues']:
                f.write("\n  - Problèmes d'intégrité référentielle:\n")
                for issue in fk_analysis['referential_integrity_issues']:
                    f.write(f"    * {issue['relation']}: {issue['missing_references_count']} références manquantes\n")
            
            f.write("\n\nDÉFINITIONS DES CONTRAINTES\n")
            f.write("-"*40 + "\n")
            for table_name, constraints in analysis_results['constraint_definitions'].items():
                f.write(f"\nTable '{table_name}':\n")
                if any(constraints.values()):
                    for constraint_type, constraint_list in constraints.items():
                        if constraint_list and constraint_type != 'table_name':
                            f.write(f"  - {constraint_type}:\n")
                            for constraint in constraint_list:
                                f.write(f"    * {constraint['column']}: {constraint['reason']}\n")
                else:
                    f.write("  - Aucune contrainte spécifique identifiée\n")
            
            f.write("\n\nÉNONCÉS SQL DDL GÉNÉRÉS\n")
            f.write("-"*40 + "\n")
            f.write("Voici les énoncés SQL pour créer les tables avec index et contraintes:\n\n")
            for statement in analysis_results['sql_ddl_statements'][:10]:  # Limiter à 10 premiers
                f.write(f"{statement}\n\n")
            if len(analysis_results['sql_ddl_statements']) > 10:
                f.write(f"... et {len(analysis_results['sql_ddl_statements']) - 10} autres énoncés\n")
        
        logger.info(f"Rapport d'analyse d'index et contraintes généré: {output_path}")


def identify_indexes_and_define_constraints():
    """
    Fonction principale pour identifier les colonnes nécessitant des index 
    et définir les contraintes d'intégrité
    """
    analyzer = IndexAndConstraintAnalyzer()
    results = analyzer.analyze_indexes_and_constraints()
    
    # Afficher un résumé dans la console
    print("="*70)
    print("ANALYSE DES INDEX ET CONTRAINTES D'INTÉGRITÉ")
    print("="*70)
    print(f"Tables analysées: {results['total_tables_analyzed']}")
    
    # Compter les éléments d'intérêt
    total_indexes = sum(len(analysis['candidate_indexes']) 
                       for analysis in results['index_analysis'].values())
    total_constraints = sum(len(constraints) 
                           for constraints in results['constraint_definitions'].values() 
                           for constraints in constraints.values() 
                           if isinstance(constraints, list))
    
    print(f"Candidates à l'indexation: {total_indexes}")
    print(f"Contraintes identifiées: {total_constraints}")
    
    # Générer le rapport détaillé
    analyzer.generate_index_constraint_report(results)
    
    return results


if __name__ == "__main__":
    identify_indexes_and_define_constraints()