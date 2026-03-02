import os
import uuid
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

import models
from database import engine, get_db

# Crear tablas al arrancar
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Sukie Card API")
templates = Jinja2Templates(directory="templates")

# Configuracion (se cambia en Railway Variables)
STAMPS_PER_REWARD = int(os.environ.get("STAMPS_PER_REWARD", "10"))
ADMIN_PIN = os.environ.get("ADMIN_PIN", "1234")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "sukie-secret-key")


# Helpers
def get_card_or_404(card_id: str, db: Session):
    try:
        card_uuid = uuid.UUID(card_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="ID de tarjeta invalido")
    card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.id == card_uuid).first()
    if not card:
        raise HTTPException(status_code=404, detail="Tarjeta no encontrada")
    return card


def verify_api_key(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="No autorizado")


# API ROUTES - usadas por Make.com

@app.post("/api/cards")
async def create_card(request: Request, db: Session = Depends(get_db)):
    """Crear tarjeta de fidelidad. Llamado por Make.com Flow 6 al registrarse un cliente."""
    verify_api_key(request)
    data = await request.json()

    email = data.get("email", "").strip().lower()
    first_name = data.get("first_name", "").strip()
    last_name = data.get("last_name", "").strip()
    shopify_id = data.get("shopify_id")

    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    # Buscar o crear cliente
    customer = db.query(models.Customer).filter(models.Customer.email == email).first()
    if not customer:
        customer = models.Customer(
            first_name=first_name or email.split("@")[0],
            last_name=last_name or "",
            email=email,
            shopify_id=shopify_id,
        )
        db.add(customer)
        db.flush()

    # Buscar o crear tarjeta
    card = db.query(models.LoyaltyCard).filter(
        models.LoyaltyCard.customer_id == customer.id
    ).first()
    if not card:
        card = models.LoyaltyCard(customer_id=customer.id)
        db.add(card)

    db.commit()
    db.refresh(card)
    db.refresh(customer)

    card_url = f"{BASE_URL}/card/{card.id}"
    return {
        "card_id": str(card.id),
        "customer_id": str(customer.id),
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "stamps": card.stamps,
        "card_url": card_url,
    }


@app.get("/api/cards/{card_id}")
async def api_get_card(card_id: str, db: Session = Depends(get_db)):
    """Obtener info de una tarjeta (JSON)."""
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "card_id": str(card.id),
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "stamps": card.stamps,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "total_stamps": card.total_stamps,
        "rewards_redeemed": card.rewards_redeemed,
        "reward_available": card.stamps >= STAMPS_PER_REWARD,
        "card_url": f"{BASE_URL}/card/{card_id}",
    }


@app.post("/api/cards/{card_id}/stamps")
async def add_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    """Anadir sellos. Requiere PIN de admin."""
    data = await request.json()
    pin = data.get("pin", "")
    stamps_to_add = int(data.get("stamps", 0))
    note = data.get("note", "")

    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    if stamps_to_add <= 0 or stamps_to_add > 100:
        raise HTTPException(status_code=400, detail="Numero de sellos invalido (1-100)")

    card = get_card_or_404(card_id, db)
    card.stamps += stamps_to_add
    card.total_stamps += stamps_to_add

    transaction = models.StampTransaction(
        card_id=card.id,
        stamps_added=stamps_to_add,
        note=note,
    )
    db.add(transaction)
    db.commit()
    db.refresh(card)

    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "success": True,
        "stamps": card.stamps,
        "stamps_added": stamps_to_add,
        "reward_available": card.stamps >= STAMPS_PER_REWARD,
        "customer_name": f"{customer.first_name} {customer.last_name or ''}".strip(),
    }


@app.post("/api/cards/{card_id}/redeem")
async def redeem_reward(card_id: str, request: Request, db: Session = Depends(get_db)):
    """Canjear premio (necesita PIN)."""
    data = await request.json()
    if data.get("pin", "") != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="PIN incorrecto")

    card = get_card_or_404(card_id, db)
    if card.stamps < STAMPS_PER_REWARD:
        raise HTTPException(status_code=400, detail="No hay suficientes sellos para el premio")

    card.stamps -= STAMPS_PER_REWARD
    card.rewards_redeemed += 1
    db.commit()
    db.refresh(card)

    return {
        "success": True,
        "stamps_remaining": card.stamps,
        "rewards_redeemed": card.rewards_redeemed,
    }


# PAGINA HTML - Vista publica de la tarjeta del cliente

@app.get("/card/{card_id}", response_class=HTMLResponse)
async def card_page(request: Request, card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    return templates.TemplateResponse("card.html", {
        "request": request,
        "card_id": card_id,
        "customer": customer,
        "card": card,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "card_url": f"{BASE_URL}/card/{card_id}",
        "reward_available": card.stamps >= STAMPS_PER_REWARD,
        "admin_pin_env": ADMIN_PIN,
    })


# PANEL ADMIN - Dashboard de todas las tarjetas

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, pin: str = "", db: Session = Depends(get_db)):
    if pin != ADMIN_PIN:
        return templates.TemplateResponse("admin_login.html", {"request": request})

    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .order_by(models.LoyaltyCard.created_at.desc())
        .all()
    )
    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request,
        "rows": rows,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "base_url": BASE_URL,
        "pin": pin,
    })


# Health check (Railway lo usa)
@app.get("/health")
def health():
    return {"status": "ok", "service": "Sukie Card"}
