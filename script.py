import kagglehub
import pandas as pd
import os

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

# ➤ Export des données nettoyées
output_path = "./output/decathlon_cleaned.csv"
df.to_csv(output_path, index=False)
print(f"\n✅ Données nettoyées exportées dans : {output_path}")
