
import pandas as pd

def merge_product_translations(products_path, translation_path, output_path):
    """
    Fusionne les produits avec leurs traductions de catégories
    """
    # Charger les fichiers
    df_products = pd.read_csv(products_path)
    df_translation = pd.read_csv(translation_path)

    # Fusionner sur la colonne de catégorie
    df_merged = pd.merge(
        df_products,
        df_translation,
        on='product_category_name',
        how='left'  # Conserve tous les produits, même sans traduction
    )

    # Sauvegarder le résultat
    df_merged.to_csv(output_path, index=False)
    print(f"Fusion terminée. Résultat sauvegardé dans : {output_path}")

if __name__ == "__main__":
    # Chemins vers les fichiers
    PRODUCTS_PATH = "data/processed/olist_products_clean.csv"
    TRANSLATION_PATH = "data/processed/product_category_name_translation_clean.csv"
    OUTPUT_PATH = "data/processed/products_with_translations.csv"

    merge_product_translations(PRODUCTS_PATH, TRANSLATION_PATH, OUTPUT_PATH)