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

import smtplib
import secrets
import hashlib
import string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

# SMTP CONFIG
SMTP_HOST     = os.environ.get("SMTP_HOST", "")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASS     = os.environ.get("SMTP_PASS", "")
SMTP_FROM     = os.environ.get("SMTP_FROM", "noreply@sukiecookie.es")
VAPID_PUBLIC  = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE = os.environ.get("VAPID_PRIVATE_KEY", "")


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
        "CREATE TABLE IF NOT EXISTS push_subscriptions (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), card_id UUID REFERENCES loyalty_cards(id), endpoint TEXT NOT NULL UNIQUE, p256dh TEXT NOT NULL, auth TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())",
        "CREATE TABLE IF NOT EXISTS referrals (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), referrer_card UUID REFERENCES loyalty_cards(id), referred_card UUID REFERENCES loyalty_cards(id), code VARCHAR(12) UNIQUE NOT NULL, used BOOLEAN DEFAULT FALSE, bonus_stamps INTEGER DEFAULT 2, created_at TIMESTAMPTZ DEFAULT NOW(), used_at TIMESTAMPTZ)",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS referral_bonus_total INTEGER DEFAULT 0",
        "ALTER TABLE loyalty_cards ADD COLUMN IF NOT EXISTS tier VARCHAR DEFAULT 'bronze'",
        # ZubCard SaaS Platform
        "CREATE TABLE IF NOT EXISTS businesses (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name VARCHAR NOT NULL, slug VARCHAR UNIQUE NOT NULL, email VARCHAR UNIQUE NOT NULL, google_id VARCHAR UNIQUE, plan VARCHAR DEFAULT 'free', card_title VARCHAR DEFAULT 'Mi Tarjeta', stamps_per_reward INTEGER DEFAULT 10, admin_pin VARCHAR NOT NULL, api_key VARCHAR NOT NULL, active BOOLEAN DEFAULT TRUE, logo_url VARCHAR, primary_color VARCHAR DEFAULT '#3A3426', accent_color VARCHAR DEFAULT '#FFF5B6', industry VARCHAR, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())",
        "INSERT INTO businesses (id, name, slug, email, admin_pin, api_key, plan, primary_color, accent_color, industry, card_title, stamps_per_reward) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Sukie Cookie', 'sukiecookie', 'zubbigpt@gmail.com', '5678', 'sukie-cookie-2026-secret', 'pro', '#3A3426', '#FFF5B6', 'bakery', 'Sukie Card', 10) ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email, admin_pin=EXCLUDED.admin_pin, api_key=EXCLUDED.api_key, plan=EXCLUDED.plan",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "ALTER TABLE card_config ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "UPDATE customers SET business_id='00000000-0000-0000-0000-000000000001'::uuid WHERE business_id IS NULL",
        "UPDATE card_config SET business_id='00000000-0000-0000-0000-000000000001'::uuid WHERE business_id IS NULL",

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


def get_tier(total_stamps: int) -> dict:
    """Returns tier info based on total lifetime stamps"""
    if total_stamps >= 150:
        return {"name": "Oro", "color": "#FFD700", "emoji": "🥇", "next": None, "next_at": None}
    elif total_stamps >= 50:
        return {"name": "Plata", "color": "#C0C0C0", "emoji": "🥈", "next": "Oro", "next_at": 150}
    else:
        return {"name": "Bronce", "color": "#CD7F32", "emoji": "🥉", "next": "Plata", "next_at": 50}


def generate_referral_code(length=8) -> str:
    chars = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))


def get_or_create_referral_code(card_id, db: Session) -> str:
    card_uuid = uuid.UUID(str(card_id))
    ref = db.query(models.Referral).filter(models.Referral.referrer_card == card_uuid).first()
    if not ref:
        code = generate_referral_code()
        # ensure unique
        while db.query(models.Referral).filter(models.Referral.code == code).first():
            code = generate_referral_code()
        ref = models.Referral(referrer_card=card_uuid, code=code)
        db.add(ref)
        db.commit()
        db.refresh(ref)
    return ref.code


# ══════════════════════════════════════════════════════════════════════════════
# ZUBCARD BUSINESS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_business_by_slug(slug: str, db: Session):
    return db.query(models.Business).filter(
        models.Business.slug == slug,
        models.Business.active == True
    ).first()


def business_api_key_auth(request: Request, business: models.Business):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {business.api_key}" and auth != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="No autorizado")


def generate_api_key() -> str:
    return "zub-" + secrets.token_urlsafe(24)


def generate_slug(name: str) -> str:
    import re as regex_mod
    slug = name.lower()
    slug = regex_mod.sub(r'[áàäâ]', 'a', slug)
    slug = regex_mod.sub(r'[éèëê]', 'e', slug)
    slug = regex_mod.sub(r'[íìïî]', 'i', slug)
    slug = regex_mod.sub(r'[óòöô]', 'o', slug)
    slug = regex_mod.sub(r'[úùüû]', 'u', slug)
    slug = regex_mod.sub(r'[ñ]', 'n', slug)
    slug = regex_mod.sub(r'[^a-z0-9]', '', slug)
    return slug[:30]



def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via SMTP. Returns True if sent, False if config missing."""
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS:
        print(f"Email NOT sent (SMTP not configured): to={to_email}, subject={subject}")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = SMTP_FROM
        msg["To"]      = to_email
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, [to_email], msg.as_string())
        print(f"✅ Email sent: to={to_email}")
        return True
    except Exception as e:
        print(f"❌ Email error: {e}")
        return False


def render_welcome_email(name: str, card_url: str, stamps: int = 0, referral_code: str = "", referral_url: str = "") -> str:
    """Render welcome email HTML"""
    template = templates.get_template("email_welcome.html")
    return template.render(
        name=name,
        card_url=card_url,
        stamps=stamps,
        referral_code=referral_code,
        referral_url=referral_url,
        subject="¡Bienvenido/a a la Sukie Card! 🍪",
    )


def render_birthday_email(name: str, card_url: str) -> str:
    template = templates.get_template("email_birthday.html")
    return template.render(name=name, card_url=card_url)


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
        "tier":           get_tier(card.total_stamps or 0),
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
def show_card(card_id: str, request: Request, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    return templates.TemplateResponse("card.html", {
        "request":        request,
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

    # Handle referral
    ref_code = body.get("ref", "").strip().upper()
    if ref_code:
        ref = db.query(models.Referral).filter(
            models.Referral.code == ref_code,
            models.Referral.used == False
        ).first()
        if ref and str(ref.referrer_card) != str(card.id):
            ref.used = True
            ref.referred_card = card.id
            ref.used_at = datetime.now(timezone.utc)
            # Give bonus stamps to referrer
            referrer_card = db.query(models.LoyaltyCard).filter(
                models.LoyaltyCard.id == ref.referrer_card).first()
            if referrer_card:
                referrer_card.stamps       = (referrer_card.stamps or 0) + ref.bonus_stamps
                referrer_card.total_stamps = (referrer_card.total_stamps or 0) + ref.bonus_stamps
            # Give bonus stamps to new user too
            card.stamps       = (card.stamps or 0) + ref.bonus_stamps
            card.total_stamps = (card.total_stamps or 0) + ref.bonus_stamps
            db.commit()


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

# ══════════════════════════════════════════════════════════════════════════════
# EMAIL PREVIEW & SEND
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/email-preview/{card_id}", response_class=HTMLResponse)
def email_preview(card_id: str, db: Session = Depends(get_db)):
    """Preview the welcome email in browser"""
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    card_url = f"{BASE_URL}/card/{card_id}"
    ref_code = get_or_create_referral_code(card_id, db)
    ref_url  = f"{BASE_URL}/register?ref={ref_code}"
    html = render_welcome_email(name, card_url, card.stamps or 0, ref_code, ref_url)
    return HTMLResponse(content=html)


@app.get("/email-preview-birthday/{card_id}", response_class=HTMLResponse)
def email_preview_birthday(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    html = render_birthday_email(name, f"{BASE_URL}/card/{card_id}")
    return HTMLResponse(content=html)


@app.post("/api/admin/send-email/{card_id}")
async def send_email_to_customer(card_id: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    email_type = body.get("type", "welcome")  # welcome | birthday
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    if not customer:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    name     = customer.first_name or "Cliente"
    card_url = f"{BASE_URL}/card/{card_id}"
    if email_type == "birthday":
        html    = render_birthday_email(name, card_url)
        subject = f"¡Feliz Cumpleaños {name}! 🎂🍪"
    else:
        ref_code = get_or_create_referral_code(card_id, db)
        ref_url  = f"{BASE_URL}/register?ref={ref_code}"
        html    = render_welcome_email(name, card_url, card.stamps or 0, ref_code, ref_url)
        subject = "¡Bienvenido/a a la Sukie Card! 🍪"
    sent = send_email(customer.email, subject, html)
    return {"sent": sent, "to": customer.email, "type": email_type,
            "note": "SMTP no configurado - configura en Railway env vars" if not sent else "Email enviado"}


@app.post("/api/admin/send-email-all")
async def send_email_all(request: Request, db: Session = Depends(get_db)):
    """Send welcome email to all customers (or just new ones)"""
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    target = body.get("target", "new")  # new | all
    rows = (db.query(models.LoyaltyCard, models.Customer)
            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
            .filter(models.Customer.email != "PLACEHOLDER@sukie.internal").all())
    sent_count = 0
    for card, cust in rows:
        if cust.email and "@" in cust.email:
            ref_code = get_or_create_referral_code(str(card.id), db)
            ref_url  = f"{BASE_URL}/register?ref={ref_code}"
            html    = render_welcome_email(cust.first_name or "Cliente",
                                           f"{BASE_URL}/card/{card.id}",
                                           card.stamps or 0, ref_code, ref_url)
            if send_email(cust.email, "Tu Sukie Card está esperándote 🍪", html):
                sent_count += 1
    return {"sent": sent_count, "total": len(rows)}


# ══════════════════════════════════════════════════════════════════════════════
# REFERIDOS
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}/referral")
def get_referral(card_id: str, db: Session = Depends(get_db)):
    """Get or create referral code for a card"""
    get_card_or_404(card_id, db)
    code = get_or_create_referral_code(card_id, db)
    used = db.query(models.Referral).filter(
        models.Referral.referrer_card == uuid.UUID(card_id),
        models.Referral.used == True
    ).count()
    return {
        "referral_code": code,
        "referral_url":  f"{BASE_URL}/register?ref={code}",
        "referrals_used": used,
        "bonus_stamps_earned": used * 2,
    }


@app.get("/api/admin/referrals")
def admin_referrals(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    refs = db.query(models.Referral).filter(models.Referral.used == True).all()
    return {"total_referrals": len(refs), "total_bonus_stamps": sum(r.bonus_stamps for r in refs)}


# ══════════════════════════════════════════════════════════════════════════════
# PUSH NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/push/subscribe")
async def push_subscribe(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    card_id_str = body.get("card_id")
    endpoint    = body.get("endpoint", "")
    p256dh      = body.get("keys", {}).get("p256dh", "")
    auth_key    = body.get("keys", {}).get("auth", "")
    if not endpoint or not p256dh or not auth_key:
        raise HTTPException(status_code=400, detail="Datos de suscripción incompletos")
    # Upsert subscription
    existing = db.query(models.PushSubscription).filter(
        models.PushSubscription.endpoint == endpoint).first()
    if existing:
        if card_id_str:
            existing.card_id = uuid.UUID(card_id_str)
        db.commit()
        return {"message": "Suscripción actualizada"}
    sub = models.PushSubscription(
        endpoint=endpoint,
        p256dh=p256dh,
        auth=auth_key,
        card_id=uuid.UUID(card_id_str) if card_id_str else None,
    )
    db.add(sub)
    db.commit()
    return {"message": "Suscripción creada"}


@app.post("/api/admin/push/send")
async def admin_push_send(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")))
    title   = body.get("title", "Sukie Cookie")
    message = body.get("message", "")
    # Log intent - actual sending requires pywebpush + VAPID keys
    subs = db.query(models.PushSubscription).count()
    return {
        "message": f"Push programado: '{title}' → '{message}'",
        "subscribers": subs,
        "note": "Para envíos reales, configura VAPID_PUBLIC_KEY y VAPID_PRIVATE_KEY en Railway y instala pywebpush",
        "vapid_public": VAPID_PUBLIC or "NO CONFIGURADO"
    }


@app.get("/api/admin/push/stats")
def push_stats(pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin)
    total = db.query(models.PushSubscription).count()
    return {"subscribers": total}


@app.get("/sw.js", response_class=HTMLResponse)
def service_worker():
    """Service worker for push notifications"""
    sw_code = """
self.addEventListener('push', function(event) {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'Sukie Cookie';
  const options = {
    body: data.body || '¡Tienes una notificación!',
    icon: '/static/icon-192.png',
    badge: '/static/badge-72.png',
    data: { url: data.url || '/' },
    requireInteraction: false,
    vibrate: [200, 100, 200]
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const url = event.notification.data && event.notification.data.url ? event.notification.data.url : '/';
  event.waitUntil(clients.openWindow(url));
});
"""
    from fastapi.responses import Response
    return Response(content=sw_code, media_type="application/javascript")


# ══════════════════════════════════════════════════════════════════════════════
# TIER INFO
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/cards/{card_id}/tier")
def get_card_tier(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    tier = get_tier(card.total_stamps or 0)
    return {
        "tier": tier,
        "total_stamps": card.total_stamps or 0,
    }


@app.get("/api/push/vapid-public")
def get_vapid_public():
    return {"key": VAPID_PUBLIC or None}



# ══════════════════════════════════════════════════════════════════════════════
# ZUBCARD SAAS PLATFORM
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def zubcard_landing(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/app/login", response_class=HTMLResponse)
async def app_login(request: Request):
    return templates.TemplateResponse("app_login.html", {"request": request})


@app.get("/app/register", response_class=HTMLResponse)
async def app_register_page(request: Request):
    return templates.TemplateResponse("app_register.html", {"request": request})


@app.post("/api/app/register")
async def register_business(request: Request, db: Session = Depends(get_db)):
    """Register a new business on ZubCard"""
    body = await request.json()
    name  = (body.get("name") or "").strip()
    email = (body.get("email") or "").strip().lower()
    pin   = str(body.get("pin") or "").strip()
    industry = body.get("industry", "retail")
    
    if not name or not email or len(pin) < 4:
        raise HTTPException(status_code=400, detail="Nombre, email y PIN (4 dígitos) requeridos")
    
    if db.query(models.Business).filter(models.Business.email == email).first():
        raise HTTPException(status_code=409, detail="Ya existe una cuenta con ese email")
    
    # Generate unique slug
    base_slug = generate_slug(name)
    slug = base_slug
    counter = 1
    while db.query(models.Business).filter(models.Business.slug == slug).first():
        slug = f"{base_slug}{counter}"
        counter += 1
    
    business = models.Business(
        name      = name,
        slug      = slug,
        email     = email,
        admin_pin = pin,
        api_key   = generate_api_key(),
        industry  = industry,
        plan      = "free",
    )
    db.add(business)
    db.commit()
    db.refresh(business)
    
    # Create their default card config
    db.execute(text(
        "INSERT INTO card_config (id, config, business_id) VALUES (gen_random_uuid(), '{}', :bid)"
    ), {"bid": str(business.id)})
    db.commit()
    
    return {
        "message":  "Negocio registrado",
        "slug":     slug,
        "name":     name,
        "dashboard_url": f"/biz/{slug}/dashboard",
        "api_key":  business.api_key,
    }


@app.post("/api/app/login")
async def login_business(request: Request, db: Session = Depends(get_db)):
    """Login to business account"""
    body  = await request.json()
    email = (body.get("email") or "").strip().lower()
    pin   = str(body.get("pin") or "").strip()
    
    biz = db.query(models.Business).filter(
        models.Business.email == email,
        models.Business.active == True
    ).first()
    
    if not biz or biz.admin_pin != pin:
        raise HTTPException(status_code=401, detail="Email o PIN incorrectos")
    
    return {
        "message":       "Login correcto",
        "slug":          biz.slug,
        "name":          biz.name,
        "plan":          biz.plan,
        "dashboard_url": f"/biz/{biz.slug}/dashboard",
    }


@app.get("/biz/{slug}/dashboard", response_class=HTMLResponse)
async def biz_dashboard(slug: str, request: Request, db: Session = Depends(get_db)):
    """Business-specific admin dashboard"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return templates.TemplateResponse("dashboard_admin.html", {
        "request":    request,
        "biz_slug":   slug,
        "biz_api_base": BASE_URL,
        "biz_name":   biz.name,
        "biz_id":     str(biz.id),
        "biz_pin":    "",  # don't expose - let JS handle login
        "biz_api_key": biz.api_key,
        "stamps_per_reward": biz.stamps_per_reward,
        "card_title": biz.card_title,
    })


@app.get("/biz/{slug}/register", response_class=HTMLResponse)
async def biz_register_page(slug: str, request: Request, ref: str = "", db: Session = Depends(get_db)):
    """Business-specific customer registration page"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return templates.TemplateResponse("register.html", {
        "request":           request,
        "card_title":        biz.card_title,
        "api_base":          BASE_URL,
        "stamps_per_reward": biz.stamps_per_reward,
        "biz_slug":          slug,
        "ref":               ref,
    })


@app.get("/biz/{slug}/card/{card_id}", response_class=HTMLResponse)
async def biz_card(slug: str, card_id: str, request: Request, db: Session = Depends(get_db)):
    """Business-specific customer card page"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    return templates.TemplateResponse("card.html", {
        "request":           request,
        "card_id":           card_id,
        "name":              name,
        "stamps":            card.stamps or 0,
        "stamps_per_reward": biz.stamps_per_reward,
        "rewards_redeemed":  card.rewards_redeemed or 0,
        "award_balance":     card.award_balance or 0,
        "total_stamps":      card.total_stamps or 0,
        "biz_name":          biz.name,
        "biz_slug":          slug,
        "primary_color":     biz.primary_color,
        "accent_color":      biz.accent_color,
    })


@app.get("/api/app/businesses")
def list_businesses(pin: str = "", db: Session = Depends(get_db)):
    """Super admin: list all businesses (only accessible with master PIN)"""
    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    bizs = db.query(models.Business).order_by(models.Business.created_at.desc()).all()
    result = []
    for b in bizs:
        count = db.query(models.Customer).filter(
            models.Customer.business_id == b.id
        ).count() if b.id else 0
        result.append({
            "id":         str(b.id),
            "name":       b.name,
            "slug":       b.slug,
            "email":      b.email,
            "plan":       b.plan,
            "industry":   b.industry,
            "customers":  count,
            "dashboard":  f"/biz/{b.slug}/dashboard",
            "created_at": b.created_at.isoformat() if b.created_at else "",
        })
    return {"businesses": result, "total": len(result)}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    return templates.TemplateResponse("dashboard_admin.html", {"request": request})
