import os
import uuid
import csv
import io
from fastapi import FastAPI, Depends, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, func as sqlfunc, or_
from datetime import datetime, timezone

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


# ── MIGRACIÓN AUTOMÁTICA ──────────────────────────────────────────────────────
@app.on_event("startup")
def run_migrations():
    """Añade columnas nuevas a tablas existentes sin romper datos."""
    migrations = [
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS phone VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS birth_date VARCHAR",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS card_active BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in_email BOOLEAN DEFAULT TRUE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_in_sms BOOLEAN DEFAULT FALSE",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
        "ALTER TABLE loyalty_cards ADD COLUMN IF NOT EXISTS award_balance INTEGER DEFAULT 0",
        "ALTER TABLE stamp_transactions ADD COLUMN IF NOT EXISTS transaction_type VARCHAR DEFAULT 'stamp'",
        # Rellenar card_active=true para registros existentes
        "UPDATE customers SET card_active=TRUE WHERE card_active IS NULL",
        "UPDATE customers SET opt_in=TRUE WHERE opt_in IS NULL",
        "UPDATE customers SET opt_in_email=TRUE WHERE opt_in_email IS NULL",
        "UPDATE customers SET opt_in_sms=FALSE WHERE opt_in_sms IS NULL",
        "UPDATE loyalty_cards SET award_balance=0 WHERE award_balance IS NULL",
        "UPDATE stamp_transactions SET transaction_type='stamp' WHERE transaction_type IS NULL",
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
        raise HTTPException(status_code=400, detail="ID de tarjeta inválido")
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


def card_to_dict(card: models.LoyaltyCard, customer: models.Customer) -> dict:
    """Serializa tarjeta + cliente en el formato completo (estilo LoyiCard)."""
    # Calcular sellos vigentes en la tarjeta actual (0..STAMPS_PER_REWARD-1)
    current_stamps = card.stamps % STAMPS_PER_REWARD if card.stamps > 0 else card.stamps
    # Si stamps == 0 y award_balance > 0, la tarjeta acaba de reiniciarse
    full_name = f"{customer.first_name or ''} {customer.last_name or ''}".strip()
    return {
        "id": str(card.id),
        "customerId": str(customer.id),
        "cardNumber": str(card.id)[:8].upper(),
        "firstName": customer.first_name or "",
        "lastName": customer.last_name or "",
        "name": full_name,
        "email": customer.email,
        "phone": customer.phone or "",
        "birthDate": customer.birth_date or "",
        "cardActive": customer.card_active if customer.card_active is not None else True,
        "optIn": customer.opt_in if customer.opt_in is not None else True,
        "optInEmail": customer.opt_in_email if customer.opt_in_email is not None else True,
        "optInSMS": customer.opt_in_sms if customer.opt_in_sms is not None else False,
        "shopifyId": customer.shopify_id or "",
        "cardUrl": f"{BASE_URL}/card/{card.id}",
        "stamps": card.stamps,
        "stampsOnCard": STAMPS_PER_REWARD,
        "totalStamps": card.total_stamps or 0,
        "awardBalance": card.award_balance or 0,
        "rewardsRedeemed": card.rewards_redeemed or 0,
        "awardTotal": (card.rewards_redeemed or 0) + (card.award_balance or 0),
        "createdAt": customer.created_at.isoformat() if customer.created_at else "",
        "updatedAt": card.updated_at.isoformat() if card.updated_at else "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── HEALTH ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/health")
def health():
    return {"status": "ok", "service": "Sukie Card"}


# ══════════════════════════════════════════════════════════════════════════════
# ── PÚBLICO: TARJETA ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/card/{card_id}", response_class=HTMLResponse)
def show_card(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    stamps_display = card.stamps % STAMPS_PER_REWARD if card.stamps >= STAMPS_PER_REWARD else card.stamps
    return templates.TemplateResponse("card.html", {
        "request": {},
        "card_id": card_id,
        "name": name,
        "stamps": stamps_display,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "rewards_redeemed": card.rewards_redeemed,
        "award_balance": card.award_balance or 0,
        "total_stamps": card.total_stamps or 0,
    })


# ══════════════════════════════════════════════════════════════════════════════
# ── API: CREAR TARJETA (Make.com / Shopify webhook) ───────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards")
async def create_card(request: Request, db: Session = Depends(get_db)):
    verify_api_key(request)
    body = await request.json()
    email = body.get("email", "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    # ¿Ya existe?
    existing = db.query(models.Customer).filter(models.Customer.email == email).first()
    if existing:
        card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.customer_id == existing.id).first()
        if card:
            return {
                "message": "Ya existe",
                "card_id": str(card.id),
                "card_url": f"{BASE_URL}/card/{card.id}",
            }

    customer = models.Customer(
        email      = email,
        first_name = body.get("first_name") or body.get("name", "").split()[0] if body.get("name") else body.get("first_name", ""),
        last_name  = body.get("last_name") or (" ".join(body.get("name", "").split()[1:]) if body.get("name") else ""),
        shopify_id = body.get("shopify_id"),
        phone      = body.get("phone", ""),
        birth_date = body.get("birth_date", ""),
        card_active= True,
        opt_in     = body.get("opt_in", True),
        opt_in_email=body.get("opt_in_email", True),
        opt_in_sms = body.get("opt_in_sms", False),
    )
    db.add(customer)
    db.flush()

    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)
    db.commit()
    db.refresh(card)
    return {
        "message": "Tarjeta creada",
        "card_id": str(card.id),
        "card_url": f"{BASE_URL}/card/{card.id}",
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API: OBTENER TARJETA POR ID ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}")
def get_card(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return card_to_dict(card, customer)


# ══════════════════════════════════════════════════════════════════════════════
# ── API: AÑADIR SELLOS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/stamps")
async def add_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    pin   = str(body.get("pin", ""))
    verify_pin(pin)
    card = get_card_or_404(card_id, db)

    n = int(body.get("stamps", 1))
    if n < 1 or n > 20:
        raise HTTPException(status_code=400, detail="stamps debe estar entre 1 y 20")

    card.stamps       = (card.stamps or 0) + n
    card.total_stamps = (card.total_stamps or 0) + n

    # ¿Alcanzó el umbral? → generar premio(s)
    awards_earned = 0
    while card.stamps >= STAMPS_PER_REWARD:
        card.stamps       -= STAMPS_PER_REWARD
        card.award_balance = (card.award_balance or 0) + 1
        awards_earned     += 1

    tx = models.StampTransaction(
        card_id          = card.id,
        stamps_added     = n,
        transaction_type = "stamp",
        note             = body.get("note", f"+{n} sello(s)"),
    )
    db.add(tx)
    db.commit()
    db.refresh(card)

    return {
        "message"      : f"+{n} sello(s) añadidos",
        "stamps"       : card.stamps,
        "total_stamps" : card.total_stamps,
        "award_balance": card.award_balance,
        "awards_earned": awards_earned,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API: QUITAR SELLOS (ajuste) ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/remove-stamps")
async def remove_stamps(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)

    n = int(body.get("stamps", 1))
    if n < 1:
        raise HTTPException(status_code=400, detail="stamps debe ser ≥ 1")

    card.stamps = max(0, (card.stamps or 0) - n)

    tx = models.StampTransaction(
        card_id          = card.id,
        stamps_added     = -n,
        transaction_type = "adjust",
        note             = body.get("note", f"-{n} sello(s) ajuste"),
    )
    db.add(tx)
    db.commit()
    db.refresh(card)

    return {"message": f"-{n} sello(s) eliminados", "stamps": card.stamps}


# ══════════════════════════════════════════════════════════════════════════════
# ── API: CANJEAR PREMIO ───────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/cards/{card_id}/redeem")
async def redeem(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    card = get_card_or_404(card_id, db)

    if (card.award_balance or 0) < 1:
        raise HTTPException(status_code=400, detail="No hay premios disponibles para canjear")

    card.award_balance    = (card.award_balance or 0) - 1
    card.rewards_redeemed = (card.rewards_redeemed or 0) + 1

    tx = models.StampTransaction(
        card_id          = card.id,
        stamps_added     = 0,
        transaction_type = "redeem",
        note             = body.get("note", "Premio canjeado 🍪"),
    )
    db.add(tx)
    db.commit()
    db.refresh(card)

    return {
        "message"         : "Premio canjeado correctamente 🍪",
        "award_balance"   : card.award_balance,
        "rewards_redeemed": card.rewards_redeemed,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API: HISTORIAL DE TRANSACCIONES ──────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}/history")
def card_history(card_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    card = get_card_or_404(card_id, db)
    txs = (
        db.query(models.StampTransaction)
        .filter(models.StampTransaction.card_id == card.id)
        .order_by(models.StampTransaction.created_at.desc())
        .limit(100)
        .all()
    )
    return {
        "history": [
            {
                "id"              : str(t.id),
                "stamps_added"    : t.stamps_added,
                "transaction_type": t.transaction_type or "stamp",
                "note"            : t.note,
                "created_at"      : t.created_at.isoformat() if t.created_at else "",
            }
            for t in txs
        ]
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: LISTAR CLIENTES ────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/customers")
def list_customers(
    pin       : str = "",
    search    : str = "",
    active    : str = "",          # "true" | "false" | ""
    sort_by   : str = "created_at",
    sort_order: str = "desc",
    page      : int = Query(1, ge=1),
    page_size : int = Query(200, ge=1, le=500),
    db        : Session = Depends(get_db),
):
    verify_pin(pin)

    q = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
    )

    # Filtros
    if search:
        like = f"%{search}%"
        q = q.filter(
            or_(
                models.Customer.email.ilike(like),
                models.Customer.first_name.ilike(like),
                models.Customer.last_name.ilike(like),
                models.Customer.phone.ilike(like),
            )
        )
    if active == "true":
        q = q.filter(models.Customer.card_active == True)
    elif active == "false":
        q = q.filter(models.Customer.card_active == False)

    # Ordenar
    sort_map = {
        "created_at"  : models.Customer.created_at,
        "updated_at"  : models.LoyaltyCard.updated_at,
        "first_name"  : models.Customer.first_name,
        "email"       : models.Customer.email,
        "stamps"      : models.LoyaltyCard.stamps,
        "total_stamps": models.LoyaltyCard.total_stamps,
        "award_balance": models.LoyaltyCard.award_balance,
    }
    col = sort_map.get(sort_by, models.Customer.created_at)
    q   = q.order_by(col.desc() if sort_order == "desc" else col.asc())

    total = q.count()
    rows  = q.offset((page - 1) * page_size).limit(page_size).all()

    customers = [card_to_dict(card, cust) for card, cust in rows]

    # ── Stats globales (sobre todos los resultados filtrados, no solo la página) ──
    all_rows = q.all() if total <= 2000 else rows   # evitar memoria en BD muy grandes
    total_stamps_all   = sum((r[0].total_stamps or 0) for r in all_rows)
    total_award_bal    = sum((r[0].award_balance or 0) for r in all_rows)
    total_redeemed     = sum((r[0].rewards_redeemed or 0) for r in all_rows)
    active_count       = sum(1 for r in all_rows if r[1].card_active)

    return {
        "customers"   : customers,
        "total"       : total,
        "page"        : page,
        "page_size"   : page_size,
        "total_pages" : (total + page_size - 1) // page_size,
        "stats": {
            "total_customers" : total,
            "active_cards"    : active_count,
            "total_stamps"    : total_stamps_all,
            "award_balance"   : total_award_bal,
            "total_redeemed"  : total_redeemed,
            "stamps_per_reward": STAMPS_PER_REWARD,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: DETALLE DE UN CLIENTE ─────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/customers/{card_id}")
def get_customer_detail(card_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return card_to_dict(card, customer)


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: CREAR CLIENTE ──────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/customers")
async def create_customer_admin(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))

    email = body.get("email", "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    existing = db.query(models.Customer).filter(models.Customer.email == email).first()
    if existing:
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese email")

    fn = body.get("first_name") or body.get("name", "").split()[0] if body.get("name") else body.get("first_name", "Cliente")
    ln = body.get("last_name")  or (" ".join(body.get("name", "").split()[1:]) if body.get("name") else "")

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
    )
    db.add(customer)
    db.flush()

    card = models.LoyaltyCard(customer_id=customer.id)
    db.add(card)
    db.commit()
    db.refresh(card)

    return {
        "message"  : "Cliente creado",
        "card_id"  : str(card.id),
        "card_url" : f"{BASE_URL}/card/{card.id}",
        "customer" : card_to_dict(card, customer),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: ACTUALIZAR CLIENTE ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.put("/api/admin/customers/{card_id}")
async def update_customer(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))

    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    # Campos actualizables
    if "first_name" in body: customer.first_name  = body["first_name"]
    if "last_name"  in body: customer.last_name   = body["last_name"]
    if "phone"      in body: customer.phone       = body["phone"]
    if "birth_date" in body: customer.birth_date  = body["birth_date"]
    if "card_active" in body: customer.card_active = bool(body["card_active"])
    if "opt_in"     in body: customer.opt_in      = bool(body["opt_in"])
    if "opt_in_email" in body: customer.opt_in_email = bool(body["opt_in_email"])
    if "opt_in_sms" in body: customer.opt_in_sms  = bool(body["opt_in_sms"])

    db.commit()
    db.refresh(customer)

    return {"message": "Cliente actualizado", "customer": card_to_dict(card, customer)}


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: EXPORTAR CSV ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/export-csv")
def export_csv(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
        .order_by(models.Customer.created_at.desc())
        .all()
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "ID Tarjeta", "Nombre", "Apellidos", "Email", "Teléfono", "Fecha Nacimiento",
        "Activa", "OptIn", "Sellos Actuales", "Sellos Totales",
        "Premios Disponibles", "Premios Canjeados", "Registro",
    ])
    for card, cust in rows:
        writer.writerow([
            str(card.id),
            cust.first_name or "",
            cust.last_name or "",
            cust.email,
            cust.phone or "",
            cust.birth_date or "",
            "Sí" if (cust.card_active is not False) else "No",
            "Sí" if (cust.opt_in is not False) else "No",
            card.stamps or 0,
            card.total_stamps or 0,
            card.award_balance or 0,
            card.rewards_redeemed or 0,
            cust.created_at.strftime("%Y-%m-%d") if cust.created_at else "",
        ])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sukiecard_clientes.csv"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: BUSCAR POR EMAIL ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/search")
def search_customer(pin: str = "", q: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Introduce al menos 2 caracteres")
    like = f"%{q}%"
    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .filter(
            or_(
                models.Customer.email.ilike(like),
                models.Customer.first_name.ilike(like),
                models.Customer.last_name.ilike(like),
                models.Customer.phone.ilike(like),
            )
        )
        .limit(20)
        .all()
    )
    return {"results": [card_to_dict(c, cu) for c, cu in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# ── API ADMIN: ESTADÍSTICAS ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/stats")
def admin_stats(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
        .all()
    )
    total     = len(rows)
    active    = sum(1 for _, c in rows if c.card_active is not False)
    t_stamps  = sum((card.total_stamps or 0) for card, _ in rows)
    t_balance = sum((card.award_balance or 0) for card, _ in rows)
    t_redeem  = sum((card.rewards_redeemed or 0) for card, _ in rows)

    # Actividad reciente (últimos 30 días)
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    recent_txs = (
        db.query(models.StampTransaction)
        .filter(models.StampTransaction.created_at >= cutoff)
        .all()
    )
    stamps_30d  = sum(t.stamps_added for t in recent_txs if (t.stamps_added or 0) > 0)
    redeems_30d = sum(1 for t in recent_txs if t.transaction_type == "redeem")

    return {
        "total_customers" : total,
        "active_cards"    : active,
        "total_stamps"    : t_stamps,
        "award_balance"   : t_balance,
        "total_redeemed"  : t_redeem,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "stamps_last_30d" : stamps_30d,
        "redeems_last_30d": redeems_30d,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── ADMIN LEGADO (HTML simple con PIN en URL) ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, pin: str = "", db: Session = Depends(get_db)):
    if pin != ADMIN_PIN:
        return templates.TemplateResponse("admin_login.html", {"request": request})
    rows = (
        db.query(models.LoyaltyCard, models.Customer)
        .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
        .filter(models.Customer.email != "PLACEHOLDER@sukie.internal")
        .order_by(models.Customer.created_at.desc())
        .all()
    )
    cards_data = [
        {
            "card_id"      : str(card.id),
            "name"         : f"{cust.first_name} {cust.last_name or ''}".strip(),
            "email"        : cust.email,
            "stamps"       : card.stamps,
            "total_stamps" : card.total_stamps or 0,
            "award_balance": card.award_balance or 0,
            "redeemed"     : card.rewards_redeemed,
            "card_url"     : f"{BASE_URL}/card/{card.id}",
        }
        for card, cust in rows
    ]
    return templates.TemplateResponse("admin_dashboard.html", {
        "request": request,
        "cards"  : cards_data,
        "total"  : len(cards_data),
        "pin"    : pin,
    })


# ══════════════════════════════════════════════════════════════════════════════
# ── DASHBOARD ADMIN RICO ──────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    """Dashboard admin completo – acceso protegido por PIN en el propio HTML."""
    return templates.TemplateResponse("dashboard_admin.html", {"request": request})
