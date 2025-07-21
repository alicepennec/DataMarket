import pandas as pd
import os

# Récupération des 3 tables cibles
customers_path = './output/customers.csv'
products_path = './output/products.csv'
ventes_path = './ventes.csv'

customers_df = pd.read_csv(customers_path)
products_df = pd.read_csv(products_path)
ventes_df = pd.read_csv(ventes_path)

