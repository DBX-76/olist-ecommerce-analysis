"""
Script pour créer un schéma relationnel documenté avec clés primaires/étrangères
"""
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import logging
from typing import Dict, List, Tuple, Any
import json

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RelationalSchemaGenerator:
    """
    Classe pour générer un schéma relationnel documenté
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialise le générateur de schéma avec la configuration
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_data_path = Path(self.config['paths']['processed_data'])
        
    def analyze_table_structure(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Analyse la structure d'une table pour le schéma relationnel
        """
        logger.info(f"Analyse de la structure pour la table: {table_name}")
        
        table_info = {
            'name': table_name,
            'description': '',
            'columns': [],
            'primary_keys': [],
            'relationships': []
        }
        
        # Analyser chaque colonne
        for col in df.columns:
            col_info = {
                'name': col,
                'data_type': str(df[col].dtype),
                'nullable': df[col].isna().any(),
                'unique': df[col].is_unique,
                'description': '',
                'is_primary_key': False,
                'is_foreign_key': False
            }
            
            # Déterminer si c'est une clé primaire potentielle
            if df[col].is_unique and not df[col].isna().any():
                if 'id' in col.lower():
                    col_info['is_primary_key'] = True
                    table_info['primary_keys'].append(col)
            
            table_info['columns'].append(col_info)
        
        # Déterminer une description de base pour la table
        if 'customer' in table_name.lower():
            table_info['description'] = 'Table contenant les informations sur les clients'
        elif 'seller' in table_name.lower():
            table_info['description'] = 'Table contenant les informations sur les vendeurs'
        elif 'order' in table_name.lower():
            table_info['description'] = 'Table contenant les informations sur les commandes'
        elif 'zip' in table_name.lower() or 'geolocation' in table_name.lower():
            table_info['description'] = 'Table de référence géographique'
        else:
            table_info['description'] = f'Table {table_name} - Informations diverses'
        
        return table_info
    
    def identify_relationships(self, tables: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Identifie les relations entre les tables
        """
        logger.info("Identification des relations entre tables")
        
        relationships = []
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
                            table1_values = set(table1[col1].dropna().unique())
                            table2_values = set(table2[col2].dropna().unique())
                            
                            # Si toutes les valeurs de table1 sont dans table2, c'est probablement une FK
                            if table1_values.issubset(table2_values):
                                relationship = {
                                    'parent_table': table2_name,
                                    'child_table': table1_name,
                                    'parent_column': col2,
                                    'child_column': col1,
                                    'relationship_type': 'ONE_TO_MANY',
                                    'description': f'{table1_name}.{col1} référence {table2_name}.{col2}'
                                }
                                relationships.append(relationship)
                            elif table2_values.issubset(table1_values):
                                relationship = {
                                    'parent_table': table1_name,
                                    'child_table': table2_name,
                                    'parent_column': col1,
                                    'child_column': col2,
                                    'relationship_type': 'ONE_TO_MANY',
                                    'description': f'{table2_name}.{col2} référence {table1_name}.{col1}'
                                }
                                relationships.append(relationship)
        
        # Ajouter des relations spécifiques basées sur le contexte métier
        # (suppositions basées sur les noms de tables typiques d'un système e-commerce)
        specific_relationships = [
            {
                'parent_table': 'zip_code_reference',
                'child_table': 'customers_with_geolocation',
                'parent_column': 'zip_code_prefix',
                'child_column': 'customer_zip_code_prefix',
                'relationship_type': 'ONE_TO_MANY',
                'description': 'Les clients appartiennent à des zones géographiques'
            },
            {
                'parent_table': 'zip_code_reference',
                'child_table': 'sellers_with_geolocation',
                'parent_column': 'zip_code_prefix',
                'child_column': 'seller_zip_code_prefix',
                'relationship_type': 'ONE_TO_MANY',
                'description': 'Les vendeurs appartiennent à des zones géographiques'
            }
        ]
        
        # Filtrer pour ne garder que les relations avec des tables existantes
        for rel in specific_relationships:
            if rel['parent_table'] in tables and rel['child_table'] in tables:
                if (rel['parent_column'] in tables[rel['parent_table']].columns and 
                    rel['child_column'] in tables[rel['child_table']].columns):
                    # Vérifier si cette relation n'existe pas déjà
                    exists = False
                    for existing_rel in relationships:
                        if (existing_rel['parent_table'] == rel['parent_table'] and
                            existing_rel['child_table'] == rel['child_table'] and
                            existing_rel['parent_column'] == rel['parent_column'] and
                            existing_rel['child_column'] == rel['child_column']):
                            exists = True
                            break
                    
                    if not exists:
                        relationships.append(rel)
        
        return relationships
    
    def generate_relational_schema(self) -> Dict[str, Any]:
        """
        Génère le schéma relationnel complet
        """
        logger.info("Génération du schéma relationnel")
        
        # Charger toutes les tables
        tables = self.load_tables()
        
        if not tables:
            logger.warning("Aucune table de données trouvée")
            return {"error": "Aucune table de données trouvée"}
        
        schema = {
            "schema_name": "Brazilian_Ecommerce_Database_Schema",
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_tables": len(tables),
            "tables": [],
            "relationships": [],
            "entity_relationship_diagram_hints": []
        }
        
        # Analyser chaque table
        for table_name, df in tables.items():
            logger.info(f"Analyse de la table: {table_name}")
            
            table_info = self.analyze_table_structure(df, table_name)
            schema["tables"].append(table_info)
        
        # Identifier les relations entre tables
        relationships = self.identify_relationships(tables)
        schema["relationships"] = relationships
        
        # Générer des indices pour le diagramme entité-relation
        er_hints = self.generate_er_diagram_hints(tables, relationships)
        schema["entity_relationship_diagram_hints"] = er_hints
        
        logger.info("Schéma relationnel généré avec succès")
        
        return schema
    
    def load_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Charge toutes les tables de données traitées
        """
        logger.info("Chargement des tables de données pour le schéma relationnel")
        
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
    
    def generate_er_diagram_hints(self, tables: Dict[str, pd.DataFrame], relationships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Génère des indices pour créer un diagramme entité-relation
        """
        hints = []
        
        # Regrouper les tables par type (dimension/fait)
        entity_categories = {
            'dimension': [],
            'fact': [],
            'reference': [],
            'other': []
        }
        
        for table_name in tables.keys():
            if any(dim_term in table_name.lower() for dim_term in ['customer', 'seller', 'product', 'category']):
                entity_categories['dimension'].append(table_name)
            elif any(fact_term in table_name.lower() for fact_term in ['order', 'item', 'payment', 'review']):
                entity_categories['fact'].append(table_name)
            elif any(ref_term in table_name.lower() for ref_term in ['zip', 'geolocation', 'reference']):
                entity_categories['reference'].append(table_name)
            else:
                entity_categories['other'].append(table_name)
        
        hints.append({
            'category': 'entity_grouping',
            'description': 'Suggestions pour regrouper les entités dans le diagramme ER',
            'groupings': entity_categories
        })
        
        # Suggestions pour la disposition
        hints.append({
            'category': 'layout_suggestions',
            'description': 'Suggestions pour la disposition du diagramme ER',
            'suggestions': [
                'Placer les tables de référence (comme zip_code_reference) au centre',
                'Positionner les tables de dimension autour des tables de fait',
                'Organiser les tables de fait au centre du diagramme',
                'Aligner les tables liées horizontalement ou verticalement'
            ]
        })
        
        # Liste des relations importantes à souligner
        important_relationships = []
        for rel in relationships:
            important_relationships.append({
                'from': rel['child_table'],
                'to': rel['parent_table'],
                'type': rel['relationship_type'],
                'description': rel['description']
            })
        
        hints.append({
            'category': 'important_relationships',
            'description': 'Relations importantes à mettre en évidence dans le diagramme',
            'relationships': important_relationships
        })
        
        return hints
    
    def generate_schema_documentation(self, schema: Dict[str, Any], output_path: str = None):
        """
        Génère la documentation du schéma relationnel
        """
        if output_path is None:
            output_path = self.processed_data_path / "relational_schema_documentation.txt"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("DOCUMENTATION DU SCHÉMA RELATIONNEL\n")
            f.write("="*70 + "\n")
            f.write(f"Nom du schéma: {schema['schema_name']}\n")
            f.write(f"Généré le: {schema['generated_at']}\n")
            f.write(f"Nombre total de tables: {schema['total_tables']}\n\n")
            
            f.write("TABLES DU SCHÉMA\n")
            f.write("-"*40 + "\n")
            for table in schema['tables']:
                f.write(f"\nTable: {table['name']}\n")
                f.write(f"Description: {table['description']}\n")
                f.write(f"Clés primaires: {', '.join(table['primary_keys']) if table['primary_keys'] else 'Aucune'}\n")
                f.write("Colonnes:\n")
                
                for col in table['columns']:
                    nullable_str = "NULL" if col['nullable'] else "NOT NULL"
                    unique_str = "UNIQUE" if col['unique'] else ""
                    pk_str = "PK" if col['is_primary_key'] else ""
                    fk_str = "FK" if col['is_foreign_key'] else ""
                    
                    constraints = [c for c in [nullable_str, unique_str, pk_str, fk_str] if c]
                    constraints_str = f" [{', '.join(constraints)}]" if constraints else ""
                    
                    f.write(f"  - {col['name']}: {col['data_type']}{constraints_str}\n")
            
            f.write("\n\nRELATIONS ENTRE TABLES\n")
            f.write("-"*40 + "\n")
            for rel in schema['relationships']:
                f.write(f"{rel['child_table']}.{rel['child_column']} → {rel['parent_table']}.{rel['parent_column']}\n")
                f.write(f"  Type: {rel['relationship_type']}\n")
                f.write(f"  Description: {rel['description']}\n\n")
            
            f.write("\n\nINDICATIONS POUR LE DIAGRAMME ENTITÉ-RELATION\n")
            f.write("-"*40 + "\n")
            for hint in schema['entity_relationship_diagram_hints']:
                f.write(f"\nCatégorie: {hint['category']}\n")
                f.write(f"Description: {hint['description']}\n")
                
                if hint['category'] == 'entity_grouping':
                    for group, tables in hint['groupings'].items():
                        if tables:
                            f.write(f"  {group.title()}: {', '.join(tables)}\n")
                elif hint['category'] == 'layout_suggestions':
                    for suggestion in hint['suggestions']:
                        f.write(f"  - {suggestion}\n")
                elif hint['category'] == 'important_relationships':
                    for rel in hint['relationships']:
                        f.write(f"  - {rel['from']} → {rel['to']} ({rel['type']}): {rel['description']}\n")
        
        logger.info(f"Documentation du schéma relationnel générée: {output_path}")
    
    def generate_json_schema(self, schema: Dict[str, Any], output_path: str = None):
        """
        Génère une version JSON du schéma pour une utilisation programmatique
        """
        if output_path is None:
            output_path = self.processed_data_path / "relational_schema.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Schéma relationnel JSON généré: {output_path}")
    
    def generate_sql_create_scripts(self, schema: Dict[str, Any], output_path: str = None):
        """
        Génère les scripts SQL pour créer les tables du schéma
        """
        if output_path is None:
            output_path = self.processed_data_path / "sql_create_scripts.sql"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("-- SCRIPTS SQL POUR LA CRÉATION DES TABLES DU SCHÉMA RELATIONNEL\n")
            f.write(f"-- Généré le: {schema['generated_at']}\n\n")
            
            # Créer les tables
            for table in schema['tables']:
                f.write(f"CREATE TABLE {table['name']} (\n")
                
                columns_def = []
                for col in table['columns']:
                    sql_type = self.map_pandas_to_sql_type(col['data_type'])
                    col_def = f"    {col['name']} {sql_type}"
                    
                    if not col['nullable']:
                        col_def += " NOT NULL"
                    
                    if col['unique']:
                        col_def += " UNIQUE"
                    
                    if col['is_primary_key']:
                        col_def += " PRIMARY KEY"
                    
                    columns_def.append(col_def)
                
                f.write(",\n".join(columns_def))
                f.write("\n);\n\n")
            
            # Créer les relations (clés étrangères)
            for rel in schema['relationships']:
                f.write(f"ALTER TABLE {rel['child_table']} ADD CONSTRAINT FK_{rel['child_table']}_{rel['child_column']}\n")
                f.write(f"    FOREIGN KEY ({rel['child_column']}) REFERENCES {rel['parent_table']}({rel['parent_column']});\n\n")
        
        logger.info(f"Scripts SQL de création générés: {output_path}")
    
    def map_pandas_to_sql_type(self, pandas_dtype: str) -> str:
        """
        Convertit un type pandas en type SQL équivalent
        """
        if 'int' in pandas_dtype:
            return "INTEGER"
        elif 'float' in pandas_dtype:
            return "DECIMAL(15,2)"
        elif 'bool' in pandas_dtype:
            return "BOOLEAN"
        elif 'datetime' in pandas_dtype or 'timestamp' in pandas_dtype:
            return "TIMESTAMP"
        else:
            return "VARCHAR(255)"


def generate_relational_schema_documentation():
    """
    Fonction principale pour créer un schéma relationnel documenté 
    avec clés primaires/étrangères
    """
    generator = RelationalSchemaGenerator()
    schema = generator.generate_relational_schema()
    
    # Afficher un résumé dans la console
    print("="*70)
    print("GÉNÉRATION DU SCHÉMA RELATIONNEL DOCUMENTÉ")
    print("="*70)
    print(f"Tables identifiées: {schema['total_tables']}")
    print(f"Relations trouvées: {len(schema['relationships'])}")
    
    print("\nTables du schéma:")
    for table in schema['tables']:
        print(f"- {table['name']} ({len(table['columns'])} colonnes)")
    
    print("\nRelations principales:")
    for rel in schema['relationships'][:5]:  # Afficher les 5 premières
        print(f"- {rel['child_table']}.{rel['child_column']} → {rel['parent_table']}.{rel['parent_column']}")
    if len(schema['relationships']) > 5:
        print(f"... et {len(schema['relationships']) - 5} autres relations")
    
    # Générer les documents
    generator.generate_schema_documentation(schema)
    generator.generate_json_schema(schema)
    generator.generate_sql_create_scripts(schema)
    
    print(f"\nDocuments générés dans: {generator.processed_data_path}")
    print("- relational_schema_documentation.txt: Documentation textuelle complète")
    print("- relational_schema.json: Version JSON pour utilisation programmatique")
    print("- sql_create_scripts.sql: Scripts SQL pour création des tables")
    
    return schema


if __name__ == "__main__":
    generate_relational_schema_documentation()