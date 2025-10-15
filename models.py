from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# --- Connexion MySQL ---
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Tables ---
class Client(Base):
    __tablename__ = "clients"

    ClientID = Column(String(50), primary_key=True)
    Sexe = Column(String(5))
    AnneeNaissance = Column(Integer)

    ventes = relationship("Vente", back_populates="client")


class Produit(Base):
    __tablename__ = "produits"

    ProduitID = Column(Integer, primary_key=True, index=True, autoincrement=True) 
    Nom = Column(String(500))
    Marque = Column(String(100))
    Prix = Column(Float)
    Couleur = Column(String(45))
    Categorie = Column(String(45))
    SousCategorie = Column(String(45))
    Pratique = Column(String(45))

    ventes = relationship("Vente", back_populates="produit")


class Vente(Base):
    __tablename__ = "ventes"

    VenteID = Column(Integer, primary_key=True)
    Date = Column(DateTime)
    ClientID = Column(String(50), ForeignKey("clients.ClientID"))
    ProduitID = Column(Integer, ForeignKey("produits.ProduitID"))

    client = relationship("Client", back_populates="ventes")
    produit = relationship("Produit", back_populates="ventes")


# Création automatique des tables (si elles n'existent pas)
#Base.metadata.create_all(bind=engine)
