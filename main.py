import os
import uuid
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import models
from database import engine, get_db

# Crear tablas al arrancar
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Sukie Card API")
templates = Jinja2Templates(directory="templates")

# ── CORS ─────────────────────────────────────────────────────────────────────
# Permite que el dashboard (HTML local o cualquier origen) llame a la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
STAMPS_PER_REWARD = int(os.environ.get("STAMPS_PER_REWARD", "10"))
ADMIN_PIN         = os.environ.get("ADMIN_PIN", "1234")
BASE_URL          = os.environ.get("BASE_URL", "http://localhost:8000")
API_KEY           = os.environ.get("API_KEY", "sukie-secret-key")


# ── HELPERS ───────────────────────────────────────────────────────────────────
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


def verify_pin(pin: str):
    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="PIN incorrecto")


# ══════════════════════════════════════════════════════════════════════════════
#  API ROUTES — usadas por Make.com y el Dashboard
# ══════════════════════════════════════════════════════════════════════════════

# ── CREAR TARJETA ─────────────────────────────────────────────────────────────
@app.post("/api/cards")
async def create_card(request: Request, db: Session = Depends(get_db)):
    """Crear tarjeta de fidelidad. Llamado por Make.com al registrarse un cliente."""
    verify_api_key(request)
    data = await request.json()

    email      = data.get("email", "").strip().lower()
    first_name = data.get("first_name", "").strip()
    last_name  = data.get("last_name", "").strip()
    shopify_id = data.get("shopify_id")

    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

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

    card = db.query(models.LoyaltyCard).filter(
        models.LoyaltyCard.customer_id == customer.id
    ).first()
    if not card:
        card = models.LoyaltyCard(customer_id=customer.id)
        db.add(card)

    db.commit()
    db.refresh(card)
    db.refresh(customer)

    return {
        "card_id":     str(card.id),
        "customer_id": str(customer.id),
        "first_name":  customer.first_name,
        "last_name":   customer.last_name,
        "email":       customer.email,
        "stamps":      card.stamps,
        "card_url":    f"{BASE_URL}/card/{card.id}",
    }


# ── CREAR CLIENTE DESDE DASHBOARD ─────────────────────────────────────────────
@app.post("/api/admin/customers")
async def create_customer_admin(request: Request, db: Session = Depends(get_db)):
    """Crear cliente manualmente desde el dashboard admin."""
    data = await request.json()
    verify_pin(data.get("pin", ""))

    email      = data.get("email", "").strip().lower()
    first_name = data.get("first_name", "").strip()
    last_name  = data.get("last_name", "").strip()

    if not email or not first_name:
        raise HTTPException(status_code=400, detail="Email y nombre requeridos")

    existing = db.query(models.Customer).filter(models.Customer.email == email).first()
    if existing:
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese email")

    customer = models.Customer(
        first_name=first_name,
        last_name=last_name or "",
        email=email,
    )
    db.add(customer)
    db.flush()

    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)
    db.commit()
    db.refresh(card)
    db.refresh(customer)

    return {
        "card_id":     str(card.id),
        "customer_id": str(customer.id),
        "first_name":  customer.first_name,
        "last_name":   customer.last_name,
        "email":       customer.email,
        "stamps":      card.stamps,
        "card_url":    f"{BASE_URL}/card/{card.id}",
        "created":     True,
    }


# ── LISTAR TODOS LOS CLIENTES ─────────────────────────────────────────────────
@app.get("/api/admin/customers")
async def list_customers(pin: str = "", db: Session = Depends(get_db)):
    """Listar todos los clientes con sus tarjetas. Requiere PIN."""
    verify_pin(pin)

    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .order_by(models.LoyaltyCard.created_at.desc())
        .all()
    )

    customers = []
    for card, customer in rows:
        # Omitir entradas placeholder
        if "PLACEHOLDER" in (customer.first_name or "") or "placeholder" in (customer.email or ""):
            continue
        customers.append({
            "card_id":          str(card.id),
            "customer_id":      str(customer.id),
            "first_name":       customer.first_name,
            "last_name":        customer.last_name or "",
            "email":            customer.email,
            "shopify_id":       customer.shopify_id,
            "stamps":           card.stamps,
            "total_stamps":     card.total_stamps,
            "rewards_redeemed": card.rewards_redeemed,
            "stamps_per_reward":STAMPS_PER_REWARD,
            "reward_available": card.stamps >= STAMPS_PER_REWARD,
            "card_url":         f"{BASE_URL}/card/{card.id}",
            "created_at":       card.created_at.isoformat() if card.created_at else None,
            "updated_at":       card.updated_at.isoformat() if card.updated_at else None,
        })

    total_stamps   = sum(c["total_stamps"]     for c in customers)
    total_rewards  = sum(c["rewards_redeemed"] for c in customers)
    active_rewards = sum(1 for c in customers if c["reward_available"])

    return {
        "customers":      customers,
        "total":          len(customers),
        "total_stamps":   total_stamps,
        "total_rewards":  total_rewards,
        "active_rewards": active_rewards,
        "stamps_per_reward": STAMPS_PER_REWARD,
    }


# ── GET TARJETA ───────────────────────────────────────────────────────────────
@app.get("/api/cards/{card_id}")
async def api_get_card(card_id: str, db: Session = Depends(get_db)):
    """Obtener info de una tarjeta (JSON)."""
    card     = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "card_id":          str(card.id),
        "first_name":       customer.first_name,
        "last_name":        customer.last_name,
        "email":            customer.email,
        "stamps":           card.stamps,
        "stamps_per_reward":STAMPS_PER_REWARD,
        "total_stamps":     card.total_stamps,
        "rewards_redeemed": card.rewards_redeemed,
        "reward_available": card.stamps >= STAMPS_PER_REWARD,
        "card_url":         f"{BASE_URL}/card/{card_id}",
    }


# ── AÑADIR SELLOS ─────────────────────────────────────────────────────────────
@app.post("/api/cards/{card_id}/stamps")
async def add_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    """Añadir sellos. Requiere PIN de admin."""
    data         = await request.json()
    pin          = data.get("pin", "")
    stamps_to_add = int(data.get("stamps", 0))
    note         = data.get("note", "")

    verify_pin(pin)
    if stamps_to_add <= 0 or stamps_to_add > 100:
        raise HTTPException(status_code=400, detail="Numero de sellos invalido (1-100)")

    card = get_card_or_404(card_id, db)
    card.stamps       += stamps_to_add
    card.total_stamps += stamps_to_add

    db.add(models.StampTransaction(card_id=card.id, stamps_added=stamps_to_add, note=note))
    db.commit()
    db.refresh(card)

    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "success":         True,
        "stamps":          card.stamps,
        "stamps_added":    stamps_to_add,
        "total_stamps":    card.total_stamps,
        "reward_available":card.stamps >= STAMPS_PER_REWARD,
        "customer_name":   f"{customer.first_name} {customer.last_name or ''}".strip(),
    }


# ── QUITAR SELLOS ─────────────────────────────────────────────────────────────
@app.post("/api/cards/{card_id}/remove-stamps")
async def remove_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    """Quitar sellos (corrección). Requiere PIN de admin."""
    data            = await request.json()
    pin             = data.get("pin", "")
    stamps_to_remove = int(data.get("stamps", 0))
    note            = data.get("note", "Corrección manual")

    verify_pin(pin)
    if stamps_to_remove <= 0 or stamps_to_remove > 100:
        raise HTTPException(status_code=400, detail="Numero de sellos invalido (1-100)")

    card = get_card_or_404(card_id, db)
    removed       = min(stamps_to_remove, card.stamps)   # no bajar de 0
    card.stamps   = max(0, card.stamps - stamps_to_remove)

    db.add(models.StampTransaction(card_id=card.id, stamps_added=-removed, note=note))
    db.commit()
    db.refresh(card)

    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "success":         True,
        "stamps":          card.stamps,
        "stamps_removed":  removed,
        "reward_available":card.stamps >= STAMPS_PER_REWARD,
        "customer_name":   f"{customer.first_name} {customer.last_name or ''}".strip(),
    }


# ── CANJEAR PREMIO ────────────────────────────────────────────────────────────
@app.post("/api/cards/{card_id}/redeem")
async def redeem_reward(card_id: str, request: Request, db: Session = Depends(get_db)):
    """Canjear premio (necesita PIN)."""
    data = await request.json()
    verify_pin(data.get("pin", ""))

    card = get_card_or_404(card_id, db)
    if card.stamps < STAMPS_PER_REWARD:
        raise HTTPException(status_code=400, detail="No hay suficientes sellos para el premio")

    card.stamps           -= STAMPS_PER_REWARD
    card.rewards_redeemed += 1

    db.add(models.StampTransaction(
        card_id=card.id,
        stamps_added=-STAMPS_PER_REWARD,
        note="Premio canjeado"
    ))
    db.commit()
    db.refresh(card)

    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "success":          True,
        "stamps_remaining": card.stamps,
        "rewards_redeemed": card.rewards_redeemed,
        "customer_name":    f"{customer.first_name} {customer.last_name or ''}".strip(),
    }


# ── HISTORIAL DE TRANSACCIONES ────────────────────────────────────────────────
@app.get("/api/cards/{card_id}/history")
async def card_history(card_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Historial de sellos de una tarjeta. Requiere PIN."""
    verify_pin(pin)
    card = get_card_or_404(card_id, db)

    transactions = (
        db.query(models.StampTransaction)
        .filter(models.StampTransaction.card_id == card.id)
        .order_by(models.StampTransaction.created_at.desc())
        .limit(50)
        .all()
    )

    return {
        "transactions": [
            {
                "id":          str(t.id),
                "stamps_added":t.stamps_added,
                "note":        t.note,
                "created_at":  t.created_at.isoformat() if t.created_at else None,
            }
            for t in transactions
        ]
    }


# ══════════════════════════════════════════════════════════════════════════════
#  PAGINAS HTML
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/card/{card_id}", response_class=HTMLResponse)
async def card_page(request: Request, card_id: str, db: Session = Depends(get_db)):
    card     = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    return templates.TemplateResponse("card.html", {
        "request":       request,
        "card_id":       card_id,
        "customer":      customer,
        "card":          card,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "card_url":      f"{BASE_URL}/card/{card_id}",
        "reward_available": card.stamps >= STAMPS_PER_REWARD,
        "admin_pin_env": ADMIN_PIN,
    })


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
        "request":           request,
        "rows":              rows,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "base_url":          BASE_URL,
        "pin":               pin,
    })


# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "Sukie Card"}


# ── DASHBOARD ADMIN COMPLETO ──────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    """Dashboard admin completo – el acceso está protegido por PIN en el propio HTML."""
    return templates.TemplateResponse("dashboard_admin.html", {"request": request})
