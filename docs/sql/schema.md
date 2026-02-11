Oui, c’est une excellente idée ! Plutôt que d’allonger le README.md principal, vous pouvez créer un fichier dédié dans un dossier docs/sql/ pour conserver une documentation claire et organisée. Voici comment procéder :

1. Créer le dossier et le fichier
mkdir -p docs/sql
2. Créer docs/sql/schema.md
Copiez-y le contenu suivant :

# Schéma de la base de données

Ce projet utilise une base de données PostgreSQL pour stocker les données Olist traitées. Voici la structure des tables et les choix techniques associés.

## Tables

### `customers`
- `customer_id` : Identifiant unique du client (VARCHAR(50), clé primaire)
- `customer_unique_id` : Identifiant unique partagé entre plusieurs commandes (VARCHAR(50))
- `customer_zip_code_prefix` : Préfixe du code postal (INT)
- `customer_city` : Ville du client (VARCHAR(100))
- `customer_state` : État du client (VARCHAR(2))
- `geolocation_lat` : Latitude (DECIMAL(9, 6))
- `geolocation_lng` : Longitude (DECIMAL(9, 6))

### `sellers`
- `seller_id` : Identifiant unique du vendeur (VARCHAR(50), clé primaire)
- `seller_zip_code_prefix` : Préfixe du code postal (INT)
- `seller_city` : Ville du vendeur (VARCHAR(100))
- `seller_state` : État du vendeur (VARCHAR(2))
- `geolocation_lat` : Latitude (DECIMAL(9, 6))
- `geolocation_lng` : Longitude (DECIMAL(9, 6))

### `orders`
- `order_id` : Identifiant unique de la commande (VARCHAR(50), clé primaire)
- `customer_id` : Identifiant du client (VARCHAR(50), clé étrangère vers `customers`)
- `order_status` : Statut de la commande (VARCHAR(20))
- `order_purchase_timestamp` : Date d'achat (TIMESTAMP)
- `order_approved_at` : Date d'approbation (TIMESTAMP)
- `order_delivered_carrier_date` : Date de livraison au transporteur (TIMESTAMP)
- `order_delivered_customer_date` : Date de livraison au client (TIMESTAMP)
- `order_estimated_delivery_date` : Date de livraison estimée (TIMESTAMP)

### `order_items`
- `order_id` : Identifiant de la commande (VARCHAR(50), clé étrangère vers `orders`)
- `order_item_id` : Numéro de l'article dans la commande (INT)
- `product_id` : Identifiant du produit (VARCHAR(50), clé étrangère vers `products`)
- `seller_id` : Identifiant du vendeur (VARCHAR(50), clé étrangère vers `sellers`)
- `shipping_limit_date` : Date limite d'expédition (TIMESTAMP)
- `price` : Prix de l'article (DECIMAL(10, 2))
- `freight_value` : Coût de livraison (DECIMAL(10, 2))
- Clé primaire composée : `(order_id, order_item_id)`

### `payments`
- `order_id` : Identifiant de la commande (VARCHAR(50), clé étrangère vers `orders`)
- `payment_sequential` : Numéro de paiement dans la commande (INT)
- `payment_type` : Type de paiement (VARCHAR(20))
- `payment_installments` : Nombre d'installments (INT)
- `payment_value` : Montant du paiement (DECIMAL(10, 2))

### `products`
- `product_id` : Identifiant unique du produit (VARCHAR(50), clé primaire)
- `product_category_name` : Catégorie du produit en portugais (VARCHAR(100))
- `product_category_name_english` : Catégorie du produit en anglais (VARCHAR(100))
- `product_name_lenght` : Longueur du nom du produit (INT)
- `product_description_lenght` : Longueur de la description du produit (INT)
- `product_photos_qty` : Nombre de photos du produit (INT)
- `product_weight_g` : Poids du produit en grammes (INT)
- `product_length_cm` : Longueur du produit en cm (INT)
- `product_height_cm` : Hauteur du produit en cm (INT)
- `product_width_cm` : Largeur du produit en cm (INT)

### `reviews`
- `review_id` : Identifiant unique de l'avis (VARCHAR(50), clé primaire)
- `order_id` : Identifiant de la commande (VARCHAR(50), clé étrangère vers `orders`)
- `review_score` : Note de l'avis (INT, entre 1 et 5)
- `review_comment_title` : Titre du commentaire (TEXT)
- `review_comment_message` : Message du commentaire (TEXT)
- `review_creation_date` : Date de création de l'avis (TIMESTAMP)
- `review_answer_timestamp` : Date de réponse à l'avis (TIMESTAMP)

## Types de données

- **Identifiants (ID)** : Les identifiants uniques (comme `customer_id`, `order_id`, etc.) sont stockés en tant que `VARCHAR(50)` dans la base de données PostgreSQL. Ces champs correspondent à des UUID hexadécimaux de 32 caractères (sans tirets) provenant des jeux de données bruts d'Olist.

  **Pourquoi `VARCHAR(50)` ?**
  - **Flexibilité** : Permet de stocker des UUID avec ou sans tirets (`-`) sans conversion.
  - **Compatibilité** : Facilite l'importation directe depuis les fichiers CSV sans manipulation supplémentaire.
  - **Performance** : Évite les conversions coûteuses liées au type natif `UUID` de PostgreSQL, qui n'apporte pas de bénéfice significatif dans ce contexte.