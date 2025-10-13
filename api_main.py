from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from models import SessionLocal, Produit, Client, Vente

app = FastAPI(title = "Decathlon DataMarket API")

# Dépendance pour la session DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally: 
        db.close()

class ProduitBase(BaseModel):
    Nom: str
    Prix: float
    Marque: Optional[str] = None
    Couleur: Optional[str] = None
    Categorie: Optional[str] = None
    SousCategorie: Optional[str] = None
    Pratique: Optional[str] = None
    
class ProduitCreate(ProduitBase):
    pass

class ProduitOut(ProduitBase):
    ProduitID: int
    class Config:
        from_attributes = True

class ClientBase(BaseModel):
    Sexe: str
    AnneeNaissance: int
    
class ClientCreate(ClientBase):
    pass

class ClientOut(ClientBase):
    ClientID: str
    class Config:
        from_attributes = True
        
class VenteBase(BaseModel):
    Date: str
    ClientID: str
    ProduitID: int
    
class VenteCreate(VenteBase):
    pass

class VenteOut(VenteBase):
    VenteID: int
    class Config:
        from_attributes = True
        
#--------------------------#
#      CRUD OPERATIONS     #
#--------------------------#

# --> READ <-- #

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l’API Decathlon DataMarket"}

# Lire tous les produits
@app.get("/datamarket/products", response_model = List[ProduitOut])
def get_products(db: Session = Depends(get_db)):
    produits = db.query(Produit).all()
    return produits

# Lire un produit
@app.get("/datamarket/products/{product_id}", response_model=ProduitOut)
def get_product(product_id: int, db: Session = Depends(get_db)):
    produit = db.query(Produit).filter(Produit.product_id == product_id).first()
    if not produit:
        raise HTTPException(status_code=404, detail="Produit non trouvé")
    return produit

# Lire tous les clients
@app.get("/datamarket/clients", response_model = List[ClientOut])
def get_clients(db: Session = Depends(get_db)):
    clients = db.query(Client).all()
    return clients

# Lire un client
@app.get("/datamarket/clients/{client_id}", response_model=ClientOut)
def get_client(client_id: int, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.client_id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client non trouvé")
    return client

# Lire toutes les ventes
@app.get("/datamarket/ventes")
def get_sales(db: Session = Depends(get_db)):
    ventes = db.query(Vente).all()
    return ventes

# Lire une vente
@app.get("/datamarket/ventes/{vente_id}", response_model=VenteOut)
def get_sale(vente_id: int, db: Session = Depends(get_db)):
    vente = db.query(Vente).filter(Vente.vente_id == vente_id).first()
    if not vente:
        raise HTTPException(status_code=404, detail="Vente non trouvée")
    return vente

# --> CREATE <-- #

# Créer un client
@app.post("/datamarket/clients", response_model=ClientOut)
def create_client(client: ClientCreate, db: Session = Depends(get_db)):
    db_client = Client(**client.dict())
    db.add(db_client)
    db.commit()
    db.refresh(db_client)
    return db_client

# Créer un produit
@app.post("/datamarket/products", response_model=ProduitOut)
def create_product(produit: ProduitCreate, db: Session = Depends(get_db)):
    db_produit = Produit(**produit.dict())
    db.add(db_produit)
    db.commit()
    db.refresh(db_produit)
    return db_produit

# Créer une vente
@app.post("/datamarket/ventes", response_model=VenteOut)
def create_sale(vente: VenteCreate, db: Session = Depends(get_db)):
    db_vente = Vente(**vente.dict())
    db.add(db_vente)
    db.commit()
    db.refresh(db_vente)
    return db_vente
    
# --> UPDATE <-- #

# Mettre à jour un produit
@app.put("/datamarket/products/{product_id}", response_model=ProduitOut)
def update_product(product_id: int, updated_data: ProduitCreate, db: Session = Depends(get_db)):
    # On cherche le produit dans la base
    produit = db.query(Produit).filter(Produit.product_id == product_id).first()
    if not produit:
        raise HTTPException(status_code=404, detail="Produit non trouvé")

    # Mise à jour des champs reçus dans la requête
    for key, value in updated_data.dict().items():
        setattr(produit, key, value)

    db.commit()
    db.refresh(produit)
    return produit
    
# --> DELETE <-- #

# Supprimer un produit
@app.delete("/datamarket/products/{product_id}")
def delete_product(product_id: int, db: Session = Depends(get_db)):
    produit = db.query(Produit).filter(Produit.product_id == product_id).first()
    if not produit:
        raise HTTPException(status_code=404, detail="Produit non trouvé")
    db.delete(produit)
    db.commit()
    return {"message": f"Produit {product_id} supprimé avec succès"}

# Supprimer un client
@app.delete("/datamarket/clients/{client_id}")
def delete_client(client_id: int, db: Session = Depends(get_db)):
    client = db.query(Client).filter(Client.client_id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client non trouvé")
    db.delete(client)
    db.commit()
    return {"message": f"Client {client_id} supprimé avec succès"}