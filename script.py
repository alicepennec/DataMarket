import kagglehub
import pandas as pd
import os
from sqlalchemy import create_engine
import pymysql
import random
from datetime import datetime, timedelta

# Étape 1 : Téléchargement du dataset
path = kagglehub.dataset_download("nikhilchadha1537/decathlon-web-scraped")
print("✔️ Dataset téléchargé depuis KaggleHub.")
print("📁 Fichiers disponibles :", os.listdir(path))

# Étape 2 : Chargement du fichier CSV
csv_file_path = os.path.join(path, "Decathlon Apparel Data.csv")
df = pd.read_csv(csv_file_path)

# Étape 3 : Exploration initiale
print("\n📊 Aperçu des données :")
print(df.head())
print("\nℹ️ Infos sur le dataset :")
print(df.info())

# Étape 4 : Nettoyage
# Suppression des colonnes inutiles (si présentes)
columns_to_drop = [col for col in df.columns if "Unnamed" in col]
df.drop(columns=columns_to_drop, inplace=True)
df.drop(columns=['MRP', 'number_of_reviews', 'product information', 'description'], inplace=True)

# Renommage standardisé (au cas où)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Nettoyage des prix (conversion string ➜ float)
if 'sale_price' in df.columns:
    df['sale_price'] = df['sale_price'].replace('[₹,]', '', regex=True).astype(float)

# Nettoyage des notes (rating)
if 'star_rating' in df.columns:
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')

# Gestion des valeurs manquantes
missing_values = df.isnull().sum()
print("\n🔍 Valeurs manquantes par colonne :")
print(missing_values[missing_values > 0])

# Suppression des doublons
nb_avant = df.shape[0]
df.drop_duplicates(inplace=True)
nb_apres = df.shape[0]
print(f"\n🧹 Doublons supprimés : {nb_avant - nb_apres}")

# Création des colonnes à enrichir
df["categorie"] = ""
df["sous_categorie"] = ""
df["pratique"] = ""

# Enrichissement simple par mots-clés
def enrich_row(row):
    title = str(row["product_name"]).lower() 

    # Catégories + sous-catégories
    if "chaussure" in title or "shoes" in title or "boots" in title:
        row["categorie"] = "Chaussures"
        row["sous_categorie"] = "Chaussures de sport"
    elif "t-shirt" in title or "tee shirt" in title or "sweatshirt" in title or "fleece" in title or "top" in title or "sleeve" in title or "base layer" in title or "shirt" in title:
        row["categorie"] = "Vêtements"
        row["sous_categorie"] = "T-shirt"
    elif "veste" in title or "jacket" in title or "cap" in title or "hoodie" in title or "softshell" in title or "gilet" in title or "vest" in title or "poncho" in title:
        row["categorie"] = "Vêtements"
        row["sous_categorie"] = "Veste"
    elif "pantalon" in title or "short" in title or "pants" in title or "tights" in title or "trousers" in title or "leggings" in title or "joggers" in title or "jogging" in title:
        row["categorie"] = "Vêtements"
        row["sous_categorie"] = "Bas"
    elif "gloves" in title or "hat" in title or "boxers" in title or "bra" in title or "backpack" in title or "socks" in title or "scarf" in title or "sunglasses" in title:
        row["categorie"] = "Accessoires"
        row["sous_categorie"] = "Divers"
    else :
        row["categorie"] = "Non déterminée"
        row["sous_categorie"] = "Non déterminée"
    # Pratiques sportives
    if "ski" in title or "snowboard" in title:
        row["pratique"] = "Ski"
    elif "randonnée" in title or "hiking" in title or "mountain" in title or "trekking" in title or "trek" in title:
        row["pratique"] = "Randonnée"
    elif "fitness" in title or "gym" in title:
        row["pratique"] = "Fitness"
    elif "running" in title or "course" in title:
        row["pratique"] = "Course à pied"
    elif "tennis" in title:
        row["pratique"] = "Sports de raquette"
    elif "cycling" in title:
        row["pratique"] = "Cyclisme"
    elif "horse" in title:
        row["pratique"] = "Equitation"
    else: 
        row["pratique"] = "Autres"

    return row

# Application de l'enrichissement
df = df.apply(enrich_row, axis=1)

# Indexation des données
df.reset_index(drop=True, inplace=True)

# Export du fichier enrichi
output_path = "./output/products.csv"
df.to_csv(output_path, index=False)
print(f"✅ Dataset enrichi exporté vers : {output_path}")

# Etape 5 : Création du dataset Clients
# Lecture du fichier clients
customers_csv_path = "./input/customers.csv" 
customers_df = pd.read_csv(customers_csv_path)

# Export du CSV simulé
customers_csv_output_path = "./output/customers.csv"
customers_df.to_csv(customers_csv_output_path, index=False)

# Etape 6 : Création du dataset Ventes
# Génération de données de vente simulées
product_ids = list(range(1, len(df) + 1))
clients = [f"c_{i}" for i in range(1, 201)]

ventes_data = []
for _ in range(1000):  # 1000 ventes
    id_prod = random.choice(product_ids)
    client_id = random.choice(clients)
    date = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 730))  # dates entre 2021 et 2023
    ventes_data.append([id_prod, date, client_id])

ventes_df = pd.DataFrame(ventes_data, columns=["product_id", "date", "client_id"])
ventes_df.reset_index(drop=True, inplace=True)
ventes_df.insert(0, 'vente_id', ventes_df.index+1)

# Export du CSV simulé
ventes_csv_path = "./output/ventes.csv"
ventes_df.to_csv(ventes_csv_path, index=False)
print(f"📦 Fichier ventes simulées exporté : {ventes_csv_path}")