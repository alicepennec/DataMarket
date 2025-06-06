import kagglehub
import pandas as pd
import os
from sqlalchemy import create_engine
import pymysql

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
# ➤ Suppression des colonnes inutiles (si présentes)
columns_to_drop = [col for col in df.columns if "Unnamed" in col]
df.drop(columns=columns_to_drop, inplace=True)

# ➤ Renommage standardisé (au cas où)
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# ➤ Nettoyage des prix (conversion string ➜ float)
if 'sale_price' in df.columns:
    df['sale_price'] = df['sale_price'].replace('[₹,]', '', regex=True).astype(float)

# ➤ Nettoyage des notes (rating)
if 'star_rating' in df.columns:
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')

# ➤ Gestion des valeurs manquantes
missing_values = df.isnull().sum()
print("\n🔍 Valeurs manquantes par colonne :")
print(missing_values[missing_values > 0])

# ➤ Suppression des doublons
nb_avant = df.shape[0]
df.drop_duplicates(inplace=True)
nb_apres = df.shape[0]
print(f"\n🧹 Doublons supprimés : {nb_avant - nb_apres}")

# ➤ Indexation des données
df.reset_index(drop=True, inplace=True)
df.insert(0, 'product_id', df.index)

# ➤ Export des données nettoyées
output_path = "./output/decathlon_cleaned.csv"
df.to_csv(output_path, index=False)
print(f"\n✅ Données nettoyées exportées dans : {output_path}")

# Création de la base de donnée
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="root"
)

with conn.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS decathlon_db;")
conn.close()

# Connexion via SQLAlchemy à la base créée
user = "root"
password = "root"
host = "localhost"
port = "3306"
database = "decathlon_db"

db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(db_url)

# Lire le csv nettoyé
df = pd.read_csv(output_path)

# Exporter vers MySQL
df.to_sql(name="products", con=engine, if_exists="replace", index=False)

print("✅ Données insérées dans la base MySQL avec succès !")
