import pandas as pd
import os
from sqlalchemy import create_engine
import pymysql

# ===============================
# Chemins vers les fichiers CSV
# ===============================
customers_path = './output/customers.csv'
products_path = './output/products.csv'
ventes_path = './output/ventes.csv'

# ===============================
# Création de la base de données
# ===============================
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="root"
)

with conn.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS decathlon_db;")
    cursor.execute("USE decathlon_db")
    
    # Table Clients
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        ClientID VARCHAR(50) PRIMARY KEY,
        Sexe VARCHAR(5),
        AnneeNaissance INT
    );
    """)
    
    # Table Produits
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS produits (
        ProduitID INT PRIMARY KEY,
        Url VARCHAR(500),
        Nom VARCHAR(500),
        Marque VARCHAR(100),
        Note FLOAT,
        Prix FLOAT,
        Couleur VARCHAR(45),
        Categorie VARCHAR(45),
        SousCategorie VARCHAR(45),
        Pratique VARCHAR(45)
    );
    """)
    
    # Table Ventes
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ventes (
        VenteID INT PRIMARY KEY,
        Date DATETIME,
        ClientID VARCHAR(50),
        ProduitID INT,
        FOREIGN KEY (ClientID) REFERENCES clients(ClientID),
        FOREIGN KEY (ProduitID) REFERENCES produits(ProduitID)
    );
    """)

conn.close()

# ===============================
# Connexion SQLAlchemy
# ===============================
user = "root"
password = "root"
host = "localhost"
port = "3306"
database = "decathlon_db"

db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(db_url)

# ===============================
# Chargement et insertion
# ===============================

# ---- Clients ----
customers_df = pd.read_csv(customers_path)

# Renommage des colonnes
customers_df.rename(columns={
    "client_id": "ClientID",
    "sex": "Sexe",
    "birth": "AnneeNaissance"
}, inplace=True)

customers_df.to_sql(name="clients", con=engine, if_exists="append", index=False)
print("✅ Table 'clients' insérée dans la base MySQL avec succès !")

# ---- Produits ----
products_df = pd.read_csv(products_path)

products_df.rename(columns={
    "product_id": "ProduitID",
    "product_url": "Url",
    "product_name": "Nom",
    "brand": "Marque",
    "star_rating": "Note",
    "sale_price": "Prix",
    "colour": "Couleur",
    "categorie": "Categorie",
    "sous_categorie": "SousCategorie",
    "pratique": "Pratique"
}, inplace=True)

products_df.to_sql(name="produits", con=engine, if_exists="append", index=False)
print("✅ Table 'produits' insérée dans la base MySQL avec succès !")

# ---- Ventes ----
ventes_df = pd.read_csv(ventes_path)

ventes_df.rename(columns={
    "vente_id": "VenteID",
    "date": "Date",
    "client_id": "ClientID",
    "product_id": "ProduitID"
}, inplace=True)

ventes_df.to_sql(name="ventes", con=engine, if_exists="append", index=False)
print("✅ Table 'ventes' insérée dans la base MySQL avec succès !")
