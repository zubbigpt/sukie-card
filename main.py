import os
import uuid
import csv
import io
import json
from fastapi import FastAPI, Depends, HTTPException, Request, Query, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, or_, func as sqlfunc
from datetime import datetime, timezone, timedelta, date

import models
from database import engine, get_db

# ── CREAR TABLAS ──────────────────────────────────────────────────────────────
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Sukie Card API")
templates = Jinja2Templates(directory="templates")

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── CONFIG ────────────────────────────────────────────────────────────────────
STAMPS_PER_REWARD = int(os.environ.get("STAMPS_PER_REWARD", "10"))
ADMIN_PIN         = os.environ.get("ADMIN_PIN", "1234")
BASE_URL          = os.environ.get("BASE_URL", "http://localhost:8000")
API_KEY           = os.environ.get("API_KEY", "sukie-secret-key")
CARD_TITLE        = os.environ.get("CARD_TITLE", "SukieCookie")
REWARD_NAME       = os.environ.get("REWARD_NAME", "Cookie Gratis 🍪")


# ── MIGRACIÓN AUTOMÁTICA ──────────────────────────────────────────────────────
@app.on_event("startup")
def run_migrations():
    migrations = [
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS phone VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS birth_date VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS card_active BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in_email BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in_sms BOOLEAN DEFAULT FALSE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS notes TEXT",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS origin VARCHAR DEFAULT 'Web'",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS channel VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS language VARCHAR DEFAULT 'es'",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS anniversary_date VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
        "ALTER TABLE loyalty_cards ADD COLUMN IF NOT EXISTS award_balance INTEGER DEFAULT 0",
        "ALTER TABLE stamp_transactions ADD COLUMN IF NOT EXISTS transaction_type VARCHAR DEFAULT 'stamp'",
        "ALTER TABLE stamp_transactions ADD COLUMN IF NOT EXISTS store VARCHAR",
        # Fix NULLs
        "UPDATE customers SET card_active=TRUE WHERE card_active IS NULL",
        "UPDATE customers SET opt_in=TRUE WHERE opt_in IS NULL",
        "UPDATE customers SET opt_in_email=TRUE WHERE opt_in_email IS NULL",
        "UPDATE customers SET opt_in_sms=FALSE WHERE opt_in_sms IS NULL",
        "UPDATE customers SET origin='API' WHERE origin IS NULL",
        "UPDATE loyalty_cards SET award_balance=0 WHERE award_balance IS NULL",
        "UPDATE stamp_transactions SET transaction_type='stamp' WHERE transaction_type IS NULL",
        "CREATE TABLE IF NOT EXISTS card_config (id INTEGER PRIMARY KEY DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())",
        "INSERT INTO card_config (id, config) SELECT 1, '{}' WHERE NOT EXISTS (SELECT 1 FROM card_config WHERE id = 1)",
    ]
    from database import SessionLocal
    db = SessionLocal()
    try:
        for sql in migrations:
            try:
                db.execute(text(sql))
            except Exception as e:
                print(f"Migration note: {e}")
        db.commit()
        print("✅ Migrations OK")
    finally:
        db.close()


# ── HELPERS ───────────────────────────────────────────────────────────────────
def get_card_or_404(card_id: str, db: Session):
    try:
        card_uuid = uuid.UUID(card_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="ID inválido")
    card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.id == card_uuid).first()
    if not card:
        raise HTTPException(status_code=404, detail="Tarjeta no encontrada")
    return card


def verify_api_key(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="No autorizado")


def verify_pin(pin: str):
    if str(pin) != str(ADMIN_PIN):
        raise HTTPException(status_code=403, detail="PIN incorrecto")


def card_to_dict(card: models.LoyaltyCard, customer: models.Customer) -> dict:
    full_name = f"{customer.first_name or ''} {customer.last_name or ''}".strip()
    return {
        "id":             str(card.id),
        "customerId":     str(customer.id),
        "cardNumber":     str(card.id)[:8].upper(),
        "firstName":      customer.first_name or "",
        "lastName":       customer.last_name or "",
        "name":           full_name,
        "email":          customer.email,
        "phone":          customer.phone or "",
        "birthDate":      customer.birth_date or "",
        "anniversaryDate": customer.anniversary_date or "",
        "cardActive":     customer.card_active if customer.card_active is not None else True,
        "optIn":          customer.opt_in if customer.opt_in is not None else True,
        "optInEmail":     customer.opt_in_email if customer.opt_in_email is not None else True,
        "optInSMS":       customer.opt_in_sms if customer.opt_in_sms is not None else False,
        "notes":          customer.notes or "",
        "origin":         customer.origin or "Web",
        "channel":        customer.channel or "",
        "language":       customer.language or "es",
        "shopifyId":      customer.shopify_id or "",
        "cardUrl":        f"{BASE_URL}/card/{card.id}",
        "stamps":         card.stamps or 0,
        "stampsOnCard":   STAMPS_PER_REWARD,
        "totalStamps":    card.total_stamps or 0,
        "awardBalance":   card.award_balance or 0,
        "rewardsRedeemed": card.rewards_redeemed or 0,
        "awardTotal":     (card.rewards_redeemed or 0) + (card.award_balance or 0),
        "createdAt":      customer.created_at.isoformat() if customer.created_at else "",
        "updatedAt":      card.updated_at.isoformat() if card.updated_at else "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/health")
def health():
    return {"status": "ok", "service": "Sukie Card"}


# ══════════════════════════════════════════════════════════════════════════════
# TARJETA PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/card/{card_id}", response_class=HTMLResponse)
def show_card(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    return templates.TemplateResponse("card.html", {
        "request":        {},
        "card_id":        card_id,
        "name":           name,
        "stamps":         card.stamps or 0,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "rewards_redeemed":  card.rewards_redeemed or 0,
        "award_balance":  card.award_balance or 0,
        "total_stamps":   card.total_stamps or 0,
    })


# ══════════════════════════════════════════════════════════════════════════════
# LANDING DE ALTA (REGISTRO PÚBLICO)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return templates.TemplateResponse("register.html", {
        "request":         request,
        "card_title":      CARD_TITLE,
        "api_base":        BASE_URL,
        "stamps_per_reward": STAMPS_PER_REWARD,
    })


@app.post("/api/register")
async def public_register(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    existing = db.query(models.Customer).filter(models.Customer.email == email).first()
    if existing:
        card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.customer_id == existing.id).first()
        return {"message": "Ya registrado", "card_id": str(card.id) if card else None,
                "card_url": f"{BASE_URL}/card/{card.id}" if card else None}

    fn = (body.get("first_name") or "").strip() or "Cliente"
    ln = (body.get("last_name") or "").strip()

    customer = models.Customer(
        email        = email,
        first_name   = fn,
        last_name    = ln,
        phone        = body.get("phone", ""),
        birth_date   = body.get("birth_date", ""),
        card_active  = True,
        opt_in       = body.get("opt_in", True),
        opt_in_email = body.get("opt_in_email", True),
        opt_in_sms   = body.get("opt_in_sms", False),
        origin       = "Web",
        channel      = body.get("channel", "Landing"),
        language     = body.get("language", "es"),
    )
    db.add(customer)
    db.flush()

    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)

    tx = models.StampTransaction(card_id=card.id, stamps_added=0,
                                  transaction_type="register", note="Alta Web")
    db.add(tx)
    db.commit()
    db.refresh(card)

    return {
        "message":  "Tarjeta creada",
        "card_id":  str(card.id),
        "card_url": f"{BASE_URL}/card/{card.id}",
        "name":     fn,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CREAR TARJETA (Make.com / Shopify webhook)
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards")
async def create_card(request: Request, db: Session = Depends(get_db)):
    verify_api_key(request)
    body = await request.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    existing = db.query(models.Customer).filter(models.Customer.email == email).first()
    if existing:
        card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.customer_id == existing.id).first()
        if card:
            return {"message": "Ya existe", "card_id": str(card.id),
                    "card_url": f"{BASE_URL}/card/{card.id}"}

    name_parts = (body.get("name") or "").split()
    fn = body.get("first_name") or (name_parts[0] if name_parts else "Cliente")
    ln = body.get("last_name") or (" ".join(name_parts[1:]) if len(name_parts) > 1 else "")

    customer = models.Customer(
        email      = email,
        first_name = fn,
        last_name  = ln,
        shopify_id = body.get("shopify_id"),
        phone      = body.get("phone", ""),
        birth_date = body.get("birth_date", ""),
        card_active= True,
        opt_in     = body.get("opt_in", True),
        opt_in_email=body.get("opt_in_email", True),
        opt_in_sms = body.get("opt_in_sms", False),
        origin     = "Shopify",
        channel    = "API",
    )
    db.add(customer)
    db.flush()

    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)
    db.commit()
    db.refresh(card)
    return {"message": "Tarjeta creada", "card_id": str(card.id),
            "card_url": f"{BASE_URL}/card/{card.id}"}


# ══════════════════════════════════════════════════════════════════════════════
# OBTENER TARJETA
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}")
def get_card(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return card_to_dict(card, customer)


# ══════════════════════════════════════════════════════════════════════════════
# AÑADIR SELLOS
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/stamps")
async def add_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)

    n = int(body.get("stamps", 1))
    if n < 0 or n > 50:
        raise HTTPException(status_code=400, detail="stamps debe estar entre 0 y 50")

    if n > 0:
        card.stamps       = (card.stamps or 0) + n
        card.total_stamps = (card.total_stamps or 0) + n

    awards_earned = 0
    while (card.stamps or 0) >= STAMPS_PER_REWARD:
        card.stamps        -= STAMPS_PER_REWARD
        card.award_balance  = (card.award_balance or 0) + 1
        awards_earned      += 1

    if n > 0:
        tx = models.StampTransaction(
            card_id=card.id, stamps_added=n,
            transaction_type="stamp",
            note=body.get("note", f"+{n} sello(s)"),
            store=body.get("store", ""),
        )
        db.add(tx)

    db.commit()
    db.refresh(card)
    return {
        "message":       f"+{n} sello(s) añadidos" if n > 0 else "OK",
        "stamps":        card.stamps,
        "total_stamps":  card.total_stamps,
        "award_balance": card.award_balance,
        "awards_earned": awards_earned,
    }


# ══════════════════════════════════════════════════════════════════════════════
# QUITAR SELLOS
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/remove-stamps")
async def remove_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)

    n = int(body.get("stamps", 1))
    card.stamps = max(0, (card.stamps or 0) - n)

    tx = models.StampTransaction(
        card_id=card.id, stamps_added=-n,
        transaction_type="adjust",
        note=body.get("note", f"-{n} sello(s) ajuste"),
    )
    db.add(tx)
    db.commit()
    db.refresh(card)
    return {"message": f"-{n} sello(s) eliminados", "stamps": card.stamps}


# ══════════════════════════════════════════════════════════════════════════════
# CANJEAR PREMIO
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/redeem")
async def redeem(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)

    if (card.award_balance or 0) < 1:
        raise HTTPException(status_code=400, detail="No hay premios disponibles")

    card.award_balance    = (card.award_balance or 0) - 1
    card.rewards_redeemed = (card.rewards_redeemed or 0) + 1

    tx = models.StampTransaction(
        card_id=card.id, stamps_added=0,
        transaction_type="redeem",
        note=body.get("note", f"Premio canjeado: {REWARD_NAME}"),
        store=body.get("store", ""),
    )
    db.add(tx)
    db.commit()
    db.refresh(card)
    return {"message": "Premio canjeado 🍪",
            "award_balance": card.award_balance,
            "rewards_redeemed": card.rewards_redeemed}


# ══════════════════════════════════════════════════════════════════════════════
# HISTORIAL
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}/history")
def card_history(card_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    card = get_card_or_404(card_id, db)
    txs = (db.query(models.StampTransaction)
           .filter(models.StampTransaction.card_id == card.id)
           .order_by(models.StampTransaction.created_at.desc())
           .limit(100).all())
    return {"history": [
        {"id": str(t.id), "stamps_added": t.stamps_added,
         "transaction_type": t.transaction_type or "stamp",
         "note": t.note, "store": t.store or "",
         "created_at": t.created_at.isoformat() if t.created_at else ""}
        for t in txs
    ]}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: LISTAR CLIENTES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/customers")
def list_customers(
    pin: str = "", search: str = "", active: str = "",
    sort_by: str = "created_at", sort_order: str = "desc",
    page: int = Query(1, ge=1), page_size: int = Query(200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    verify_pin(pin)
    q = (db.query(models.LoyaltyCard, models.Customer)
         .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
         .filter(models.Customer.email != "PLACEHOLDER@sukie.internal"))

    if search:
        like = f"%{search}%"
        q = q.filter(or_(
            models.Customer.email.ilike(like),
            models.Customer.first_name.ilike(like),
            models.Customer.last_name.ilike(like),
            models.Customer.phone.ilike(like),
        ))
    if active == "true":
        q = q.filter(models.Customer.card_active == True)
    elif active == "false":
        q = q.filter(models.Customer.card_active == False)

    sort_map = {
        "created_at": models.Customer.created_at,
        "updated_at": models.LoyaltyCard.updated_at,
        "first_name": models.Customer.first_name,
        "email": models.Customer.email,
        "stamps": models.LoyaltyCard.stamps,
        "total_stamps": models.LoyaltyCard.total_stamps,
        "award_balance": models.LoyaltyCard.award_balance,
    }
    col = sort_map.get(sort_by, models.Customer.created_at)
    q = q.order_by(col.desc() if sort_order == "desc" else col.asc())

    total = q.count()
    rows  = q.offset((page - 1) * page_size).limit(page_size).all()
    customers = [card_to_dict(card, cust) for card, cust in rows]

    return {
        "customers":   customers,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: DETALLE CLIENTE
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/customers/{card_id}")
def get_customer_detail(card_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return card_to_dict(card, customer)


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: CREAR CLIENTE
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/customers")
async def create_customer_admin(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")
    if db.query(models.Customer).filter(models.Customer.email == email).first():
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese email")

    name_parts = (body.get("name") or "").split()
    fn = body.get("first_name") or (name_parts[0] if name_parts else "Cliente")
    ln = body.get("last_name") or (" ".join(name_parts[1:]) if len(name_parts) > 1 else "")

    customer = models.Customer(
        email       = email,
        first_name  = fn or "Cliente",
        last_name   = ln or "",
        phone       = body.get("phone", ""),
        birth_date  = body.get("birth_date", ""),
        card_active = True,
        opt_in      = body.get("opt_in", True),
        opt_in_email= body.get("opt_in_email", True),
        opt_in_sms  = body.get("opt_in_sms", False),
        notes       = body.get("notes", ""),
        origin      = "Admin",
        channel     = "Manual",
    )
    db.add(customer)
    db.flush()
    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)
    db.commit()
    db.refresh(card)
    return {"message": "Cliente creado", "card_id": str(card.id),
            "card_url": f"{BASE_URL}/card/{card.id}",
            "customer": card_to_dict(card, customer)}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ACTUALIZAR CLIENTE
# ══════════════════════════════════════════════════════════════════════════════
@app.put("/api/admin/customers/{card_id}")
async def update_customer(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    fields = ["first_name", "last_name", "phone", "birth_date", "notes",
              "language", "anniversary_date"]
    bools  = ["card_active", "opt_in", "opt_in_email", "opt_in_sms"]
    for f in fields:
        if f in body: setattr(customer, f, body[f])
    for f in bools:
        if f in body: setattr(customer, f, bool(body[f]))

    db.commit()
    db.refresh(customer)
    return {"message": "Cliente actualizado", "customer": card_to_dict(card, customer)}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ELIMINAR CLIENTE
# ══════════════════════════════════════════════════════════════════════════════
@app.delete("/api/admin/customers/{card_id}")
async def delete_customer(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    # Borrar transacciones → tarjeta → cliente
    db.query(models.StampTransaction).filter(models.StampTransaction.card_id == card.id).delete()
    db.delete(card)
    if customer:
        db.delete(customer)
    db.commit()
    return {"message": "Cliente eliminado"}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: BUSCAR
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/search")
def search_customer(pin: str = "", q: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Mínimo 2 caracteres")
    like = f"%{q}%"
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(or_(
                models.Customer.email.ilike(like),
                models.Customer.first_name.ilike(like),
                models.Customer.last_name.ilike(like),
                models.Customer.phone.ilike(like),
            )).limit(20).all())
    return {"results": [card_to_dict(c, cu) for c, cu in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ESTADÍSTICAS GLOBALES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/stats")
def admin_stats(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(models.Customer.email != "PLACEHOLDER@sukie.internal").all())

    total       = len(rows)
    active      = sum(1 for _, c in rows if c.card_active is not False)
    t_stamps    = sum((card.total_stamps or 0) for card, _ in rows)
    t_balance   = sum((card.award_balance or 0) for card, _ in rows)
    t_redeem    = sum((card.rewards_redeemed or 0) for card, _ in rows)

    now = datetime.now(timezone.utc)
    cutoff30  = now - timedelta(days=30)
    cutoff60  = now - timedelta(days=60)

    txs30 = (db.query(models.StampTransaction)
             .filter(models.StampTransaction.created_at >= cutoff30).all())
    txs_prev = (db.query(models.StampTransaction)
                .filter(models.StampTransaction.created_at >= cutoff60,
                        models.StampTransaction.created_at < cutoff30).all())

    stamps_30d  = sum(t.stamps_added for t in txs30 if (t.stamps_added or 0) > 0)
    redeems_30d = sum(1 for t in txs30 if t.transaction_type == "redeem")
    stamps_prev = sum(t.stamps_added for t in txs_prev if (t.stamps_added or 0) > 0)
    redeems_prev= sum(1 for t in txs_prev if t.transaction_type == "redeem")

    new_30d  = sum(1 for _, c in rows if c.created_at and c.created_at >= cutoff30)
    new_prev = sum(1 for _, c in rows if c.created_at and cutoff60 <= c.created_at < cutoff30)

    # Awards emitidos (nuevos) en 30d
    awards_30d  = sum(t.stamps_added == 0 and t.transaction_type == "stamp" for t in txs30)
    # Better: count from stamps overflow → just approximate
    awards_issued_30d = stamps_30d // STAMPS_PER_REWARD

    return {
        "total_customers":    total,
        "active_cards":       active,
        "total_stamps":       t_stamps,
        "award_balance":      t_balance,
        "total_redeemed":     t_redeem,
        "stamps_per_reward":  STAMPS_PER_REWARD,
        "stamps_last_30d":    stamps_30d,
        "redeems_last_30d":   redeems_30d,
        "stamps_prev_30d":    stamps_prev,
        "redeems_prev_30d":   redeems_prev,
        "new_clients_30d":    new_30d,
        "new_clients_prev":   new_prev,
        "awards_issued_30d":  awards_issued_30d,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ACTIVIDAD DIARIA (últimos N días)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/activity")
def admin_activity(pin: str = "", days: int = 30, db: Session = Depends(get_db)):
    verify_pin(pin)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    txs = (db.query(models.StampTransaction)
           .filter(models.StampTransaction.created_at >= cutoff)
           .order_by(models.StampTransaction.created_at.asc())
           .all())

    # Agrupa por fecha
    by_day: dict = {}
    for t in txs:
        if not t.created_at:
            continue
        day = t.created_at.strftime("%Y-%m-%d")
        if day not in by_day:
            by_day[day] = {"date": day, "stamps": 0, "redeems": 0, "new_clients": 0}
        if (t.stamps_added or 0) > 0:
            by_day[day]["stamps"] += t.stamps_added
        if t.transaction_type == "redeem":
            by_day[day]["redeems"] += 1
        if t.transaction_type == "register":
            by_day[day]["new_clients"] += 1

    # Fill gaps
    result = []
    for i in range(days):
        d = (datetime.now(timezone.utc) - timedelta(days=days - 1 - i)).strftime("%Y-%m-%d")
        result.append(by_day.get(d, {"date": d, "stamps": 0, "redeems": 0, "new_clients": 0}))

    return {"activity": result}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: CLIENTES CON CUMPLEAÑOS HOY / ESTE MES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/birthdays")
def admin_birthdays(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    today = datetime.now().strftime("%m-%d")
    this_month = datetime.now().strftime("%m")

    all_custs = (db.query(models.Customer, models.LoyaltyCard)
                 .join(models.LoyaltyCard, models.LoyaltyCard.customer_id == models.Customer.id)
                 .filter(models.Customer.birth_date.isnot(None),
                         models.Customer.birth_date != "").all())

    today_list  = []
    month_list  = []
    for cust, card in all_custs:
        bd = cust.birth_date or ""
        if len(bd) >= 10:
            mm_dd = bd[5:10]   # "MM-DD"
            mm    = bd[5:7]
            entry = card_to_dict(card, cust)
            if mm_dd == today:
                today_list.append(entry)
            if mm == this_month:
                month_list.append(entry)

    return {
        "today":       today_list,
        "this_month":  month_list,
        "today_date":  datetime.now().strftime("%d/%m"),
        "month_name":  ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                        "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
                        ][datetime.now().month - 1],
    }


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: TOP CLIENTES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/top-customers")
def top_customers(pin: str = "", limit: int = 10, db: Session = Depends(get_db)):
    verify_pin(pin)
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
            .order_by(models.LoyaltyCard.total_stamps.desc())
            .limit(limit).all())
    return {"top": [card_to_dict(c, cu) for c, cu in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: IMPORTAR CSV
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/import-csv")
async def import_csv(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    customers_data = body.get("customers", [])

    created = 0; skipped = 0; errors = []
    for row in customers_data:
        email = (row.get("email") or "").strip().lower()
        if not email:
            errors.append(f"Fila sin email: {row}")
            continue
        if db.query(models.Customer).filter(models.Customer.email == email).first():
            skipped += 1
            continue
        try:
            fn = (row.get("first_name") or row.get("nombre") or "Cliente").strip()
            ln = (row.get("last_name") or row.get("apellidos") or "").strip()
            cust = models.Customer(
                email        = email,
                first_name   = fn,
                last_name    = ln,
                phone        = row.get("phone") or row.get("telefono") or "",
                birth_date   = row.get("birth_date") or row.get("fecha_nacimiento") or "",
                card_active  = True,
                opt_in       = True,
                opt_in_email = True,
                origin       = "Import",
                channel      = "CSV",
            )
            db.add(cust)
            db.flush()
            stamps_n = int(row.get("stamps") or row.get("sellos") or 0)
            card = models.LoyaltyCard(customer_id=cust.id, stamps=stamps_n % STAMPS_PER_REWARD,
                                       total_stamps=stamps_n)
            db.add(card)
            created += 1
        except Exception as e:
            errors.append(f"{email}: {e}")
    db.commit()
    return {"created": created, "skipped": skipped, "errors": errors}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: EXPORTAR CSV
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/export-csv")
def export_csv(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
            .order_by(models.Customer.created_at.desc()).all())
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID Tarjeta","Nombre","Apellidos","Email","Teléfono",
                     "Fecha Nacimiento","Activa","OptIn","Sellos Actuales",
                     "Sellos Totales","Premios Disponibles","Premios Canjeados",
                     "Origen","Canal","Notas","Registro"])
    for card, cust in rows:
        writer.writerow([
            str(card.id), cust.first_name or "", cust.last_name or "",
            cust.email, cust.phone or "", cust.birth_date or "",
            "Sí" if (cust.card_active is not False) else "No",
            "Sí" if (cust.opt_in is not False) else "No",
            card.stamps or 0, card.total_stamps or 0,
            card.award_balance or 0, card.rewards_redeemed or 0,
            cust.origin or "", cust.channel or "", cust.notes or "",
            cust.created_at.strftime("%Y-%m-%d") if cust.created_at else "",
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=sukiecard_clientes.csv"})


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN (HTML legado)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/admin", response_class=HTMLResponse)
async def admin_legacy(request: Request, pin: str = "", db: Session = Depends(get_db)):
    if pin != ADMIN_PIN:
        return templates.TemplateResponse("admin_login.html", {"request": request})
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
            .order_by(models.Customer.created_at.desc()).all())
    cards_data = [{
        "card_id": str(card.id),
        "name": f"{cust.first_name} {cust.last_name or ''}".strip(),
        "email": cust.email, "stamps": card.stamps,
        "total_stamps": card.total_stamps or 0,
        "award_balance": card.award_balance or 0,
        "redeemed": card.rewards_redeemed,
        "card_url": f"{BASE_URL}/card/{card.id}",
    } for card, cust in rows]
    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request, "cards": cards_data,
        "total": len(cards_data), "pin": pin,
    })


# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: CONFIG DE TARJETA
# ══════════════════════════════════════════════════════════════════════════════
DEFAULT_CONFIG = {
    "general": {
        "card_name": "Sellos",
        "card_title": "Sukie Card",
        "issuer": "Sukie Cookie",
        "description": "Tarjeta de sellos Sukie Cookie",
        "expiry_type": "never",  # never | date | days_from_register
        "expiry_date": "",
        "expiry_days": 365,
        "card_prefix": "C521",
        "barcode_type": "QR",
    },
    "programa": {
        "stamps_per_reward": 10,
        "reward_name": "Cookie Gratis",
        "msg_single": "{nombre} has conseguido 1 sello",
        "msg_multiple": "{nombre} has conseguido {#} sellos",
        "birthday_enabled": True,
        "birthday_msg_push": "¡Feliz Cumpleaños! 🎂",
        "birthday_time": "07:00",
        "anniversary_enabled": True,
    },
    "landing": {
        "form_title": "Únete a la Sukie Card",
        "header_text": "Regístrate y acumula cookies gratis",
        "button_text": "Registrarse",
        "bg_color": "#FFF5B6",
        "text_color": "#280011",
        "link_color": "#A0006B",
        "button_color": "#280011",
        "field_name": True,
        "field_lastname": True,
        "field_email": True,
        "field_phone": True,
        "field_birthdate": True,
        "field_name_required": True,
        "field_lastname_required": False,
        "field_email_required": True,
        "field_phone_required": False,
        "field_birthdate_required": False,
        "company_name": "Suculent grupo de hosteleria SL",
        "company_email": "suculentcookie@gmail.com",
        "company_phone": "683174396",
        "terms_url": "",
        "register_limit_date": "",
        "access_type": "public",
    },
    "diseno": {
        "commercial_name": "Sukie Cookie",
        "card_bg_color": "#FFFFC6",
        "label_color": "#220015",
        "text_color": "#220015",
        "stamp_bg_color": "#22000F",
        "stamp_icon_color": "#FFF5B6",
        "stamp_border_color": "#FFF5B6",
        "stamp_filled_color": "#FF6B9D",
        "front_field1_label": "Titular",
        "front_field1_value": "NOMBRE Y APELLIDOS",
        "front_field2_label": "Cookies disponibles",
        "front_field2_value": "Premios/Vales Disponibles",
        "link_instagram": "https://instagram.com/sukie.cookie",
        "link_web": "https://sukiecookie.es",
        "back_title_updates": "Últimas actualizaciones",
        "back_title_holder": "Titular",
        "back_title_rewards_pending": "Premios para canjear",
        "back_title_rewards_won": "Premios ganados",
        "back_title_how": "Cómo conseguir sellos",
        "back_title_reward_detail": "Detalles del premio",
        "back_title_links": "Enlaces de interés",
        "back_title_questions": "¿Preguntas sobre esta tarjeta?",
    },
    "comunicaciones": {
        "welcome_email_enabled": True,
        "welcome_email_subject": "¡Bienvenido/a a la Sukie Card! 🍪",
        "welcome_email_body": "Hola {nombre},\n\nYa eres parte de la familia Sukie Cookie.\nConsigue 10 sellos y llévate una cookie gratis.\n\nVer tu tarjeta: {link_tarjeta}\n\n¡Hasta pronto!\nSukie Cookie",
        "birthday_email_enabled": True,
        "birthday_email_subject": "¡Feliz Cumpleaños de parte de Sukie Cookie! 🎂🍪",
        "birthday_email_body": "Hola {nombre},\n\n¡Hoy es tu día especial!\nPasa a visitarnos y llévate un regalo.\n\nCon cariño,\nSukie Cookie",
    }
}

@app.get("/api/admin/config")
def get_config(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    from database import SessionLocal
    row = db.execute(text("SELECT config FROM card_config WHERE id=1")).fetchone()
    if row:
        try:
            stored = json.loads(row[0])
        except:
            stored = {}
    else:
        stored = {}
    # Merge with defaults
    result = {**DEFAULT_CONFIG}
    for section, values in stored.items():
        if section in result and isinstance(values, dict):
            result[section] = {**result[section], **values}
        else:
            result[section] = values
    return result


@app.put("/api/admin/config")
async def save_config(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    data = {k: v for k, v in body.items() if k != "pin"}
    db.execute(text("UPDATE card_config SET config=:cfg, updated_at=NOW() WHERE id=1"),
               {"cfg": json.dumps(data)})
    db.commit()
    return {"message": "Configuración guardada"}

# DASHBOARD RICO
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    return templates.TemplateResponse("dashboard_admin.html", {"request": request})
