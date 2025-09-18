import pandas as pd
import os
from sqlalchemy import create_engine
import pymysql

# Récupération des 3 tables cibles
customers_path = './output/customers.csv'
products_path = './output/products.csv'
ventes_path = './output/ventes.csv'

## Intégration en base de données MySQL
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

# Export des tables vers MySQL
products_df = pd.read_csv(products_path)
products_df.to_sql(name="products", con=engine, if_exists="replace", index=False)
print("✅ Table 'products' insérée dans la base MySQL avec succès !")

ventes_df = pd.read_csv(ventes_path)
ventes_df.to_sql(name="ventes", con=engine, if_exists="replace", index=False)
print("✅ Table 'ventes' insérée dans la base MySQL avec succès !")

customers_df = pd.read_csv(customers_path)
customers_df.to_sql(name="clients", con=engine, if_exists="replace", index=False)
print("✅ Table 'clients' insérée dans la base MySQL avec succès !")