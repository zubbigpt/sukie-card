import os
import uuid
import csv
import io
import json
import time
from fastapi import FastAPI, Depends, HTTPException, Request, Query, File, UploadFile, Body
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from urllib.parse import urlencode
import httpx
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

# GOOGLE OAUTH CONFIG
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "800026879544-rj3j2cces61cardtspp0oomhr58ssm8i.apps.googleusercontent.com")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", "https://web-production-cdaf7.up.railway.app/auth/google/callback")

# GOOGLE WALLET CONFIG
GOOGLE_WALLET_ISSUER_ID   = os.environ.get("GOOGLE_WALLET_ISSUER_ID", "")
GOOGLE_WALLET_CREDENTIALS = os.environ.get("GOOGLE_WALLET_CREDENTIALS", "")  # JSON string

def generate_google_wallet_url(
    card_id: str,
    biz_slug: str,
    biz_name: str,
    customer_name: str,
    stamps: int,
    stamps_per_reward: int,
    card_url: str,
    primary_color: str = "#3A3426",
) -> str | None:
    """Generate a signed Google Wallet 'Save to Wallet' URL for a loyalty pass.
    Returns None if Google Wallet env vars are not configured."""
    if not GOOGLE_WALLET_ISSUER_ID or not GOOGLE_WALLET_CREDENTIALS:
        return None
    try:
        import jwt as pyjwt
        sa = json.loads(GOOGLE_WALLET_CREDENTIALS)
        issuer_id = GOOGLE_WALLET_ISSUER_ID
        class_suffix = biz_slug.replace("-", "_")
        class_id  = f"{issuer_id}.{class_suffix}"
        object_id = f"{issuer_id}.{card_id.replace('-', '_')}"
        hex_color = primary_color if primary_color.startswith("#") else "#3A3426"
        stamps_left = max(0, stamps_per_reward - stamps)
        header_val = f"{stamps}/{stamps_per_reward} sellos"
        if stamps_left == 0:
            header_val = "🎉 ¡Premio disponible!"
        generic_object = {
            "id": object_id,
            "classId": class_id,
            "genericType": "GENERIC_TYPE_UNSPECIFIED",
            "hexBackgroundColor": hex_color,
            "cardTitle": {
                "defaultValue": {"language": "es", "value": biz_name}
            },
            "subheader": {
                "defaultValue": {"language": "es", "value": "Tarjeta de Fidelidad"}
            },
            "header": {
                "defaultValue": {"language": "es", "value": customer_name}
            },
            "textModulesData": [
                {
                    "id": "stamps",
                    "header": "Sellos",
                    "body": header_val
                }
            ],
            "barcode": {
                "type": "QR_CODE",
                "value": card_url,
                "alternateText": card_id[:8].upper()
            },
            "state": "ACTIVE",
        }
        claims = {
            "iss": sa["client_email"],
            "aud": "google",
            "typ": "savetowallet",
            "iat": int(time.time()),
            "payload": {"genericObjects": [generic_object]},
            "origins": [BASE_URL],
        }
        token = pyjwt.encode(
            claims,
            sa["private_key"],
            algorithm="RS256",
            headers={"kid": sa.get("private_key_id", "")},
        )
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return f"https://pay.google.com/gp/v/save/{token}"
    except Exception as e:
        print(f"[Google Wallet] Error generating JWT: {e}")
        return None


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
        # Force-seed Sukie Cookie: delete any conflicting rows first, then insert fresh
        "DELETE FROM businesses WHERE email='zubbigpt@gmail.com' AND slug != 'sukiecookie'",
        "DELETE FROM businesses WHERE id='00000000-0000-0000-0000-000000000001'::uuid AND slug != 'sukiecookie'",
        "INSERT INTO businesses (id, name, slug, email, admin_pin, api_key, plan, primary_color, accent_color, industry, card_title, stamps_per_reward, active) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Sukie Cookie', 'sukiecookie', 'zubbigpt@gmail.com', '5678', 'sukie-cookie-2026-secret', 'pro', '#3A3426', '#FFF5B6', 'bakery', 'Sukie Card', 10, TRUE) ON CONFLICT (slug) DO UPDATE SET id='00000000-0000-0000-0000-000000000001'::uuid, name='Sukie Cookie', email='zubbigpt@gmail.com', admin_pin='5678', api_key='sukie-cookie-2026-secret', plan='pro', active=TRUE",
        # Also force-update via plain UPDATE as safety net
        "UPDATE businesses SET active=TRUE, admin_pin='5678', email='zubbigpt@gmail.com' WHERE slug='sukiecookie'",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "ALTER TABLE card_config ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "UPDATE customers SET business_id='00000000-0000-0000-0000-000000000001'::uuid WHERE business_id IS NULL",
        "UPDATE card_config SET business_id='00000000-0000-0000-0000-000000000001'::uuid WHERE business_id IS NULL",
        "DELETE FROM customers WHERE email = 'placeholder_email' OR first_name = 'PLACEHOLDER_FNAME'",
        # Clean test customers: delete in FK order (push_subscriptions → referrals → stamp_transactions → loyalty_cards → customers)
        "DELETE FROM push_subscriptions WHERE card_id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM referrals WHERE referrer_card IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid) OR referred_card IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM stamp_transactions WHERE card_id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM loyalty_cards WHERE id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM customers WHERE id NOT IN (SELECT DISTINCT customer_id FROM loyalty_cards WHERE customer_id IS NOT NULL)",
        # Upgrade Café Luna demo account to pro
        "UPDATE businesses SET plan='pro' WHERE slug='cafeluna'",
        # ── Tiendas / Locales ──────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS stores (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, pin VARCHAR NOT NULL DEFAULT '', notes TEXT DEFAULT '', active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── PassCodes ─────────────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS passcodes (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), code VARCHAR(16) UNIQUE NOT NULL, stamps INTEGER DEFAULT 1, used BOOLEAN DEFAULT FALSE, used_by UUID REFERENCES loyalty_cards(id), used_at TIMESTAMPTZ, expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Campañas ──────────────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS campaigns (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, subject VARCHAR DEFAULT '', body TEXT DEFAULT '', type VARCHAR DEFAULT 'email', status VARCHAR DEFAULT 'draft', segment VARCHAR DEFAULT 'all', created_at TIMESTAMPTZ DEFAULT NOW(), sent_at TIMESTAMPTZ)",
        # ── Custom QRs de Alta ────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS custom_qrs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), canal VARCHAR NOT NULL, local_name VARCHAR DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Card Programs (multi-tarjeta) ────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS card_programs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, emoji VARCHAR DEFAULT '🃏', stamps_per_reward INTEGER DEFAULT 10, reward_name VARCHAR DEFAULT 'Premio', bg_color VARCHAR DEFAULT '#0a0a0a', accent_color VARCHAR DEFAULT '#00e676', text_color VARCHAR DEFAULT '#ffffff', status VARCHAR DEFAULT 'active', sort_order INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Fix card_config.id to auto-increment (multi-tenant fix) ──────────────
        "CREATE SEQUENCE IF NOT EXISTS card_config_id_seq START WITH 100",
        "ALTER TABLE card_config ALTER COLUMN id SET DEFAULT nextval('card_config_id_seq')",
        "ALTER TABLE card_config ADD COLUMN IF NOT EXISTS id_fixed BOOLEAN DEFAULT FALSE",
        # ── Unique index on card_config.business_id for safe upserts ─────────────
        "CREATE UNIQUE INDEX IF NOT EXISTS uidx_card_config_business ON card_config(business_id) WHERE business_id IS NOT NULL",

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


def verify_pin(pin: str, db: Session = None):
    """Verify PIN against global ADMIN_PIN or any business admin_pin from DB"""
    if str(pin) == str(ADMIN_PIN):
        return
    # If db provided, also check any business's admin_pin
    if db:
        biz = db.query(models.Business).filter(models.Business.admin_pin == str(pin)).first()
        if biz:
            return
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
        or_(models.Business.active == True, models.Business.active == None)
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
# PÁGINAS LEGALES
# ══════════════════════════════════════════════════════════════════════════════
PRIVACY_CONTENT = """
<h2>1. Responsable del tratamiento</h2>
<p>ZubCard (en adelante, "la Plataforma") es responsable del tratamiento de los datos personales que se recogen a través de este sitio web y de las tarjetas de fidelización gestionadas por la misma.</p>

<h2>2. Datos que recopilamos</h2>
<p>Recopilamos los siguientes datos personales de los usuarios finales que se registran en los programas de fidelización:</p>
<ul>
  <li>Nombre y apellidos</li>
  <li>Dirección de correo electrónico</li>
  <li>Número de teléfono (opcional)</li>
  <li>Fecha de nacimiento (opcional)</li>
  <li>Historial de sellos y premios</li>
</ul>

<h2>3. Finalidad del tratamiento</h2>
<p>Los datos se tratan con las siguientes finalidades:</p>
<ul>
  <li>Gestión del programa de fidelización del negocio correspondiente</li>
  <li>Envío de comunicaciones relacionadas con el programa (bienvenida, cumpleaños, campañas)</li>
  <li>Mejora del servicio y análisis estadístico agregado</li>
</ul>

<h2>4. Base legal</h2>
<p>El tratamiento se basa en el consentimiento del interesado, prestado en el momento del registro, y en la ejecución del contrato de fidelización.</p>

<h2>5. Conservación de datos</h2>
<p>Los datos se conservan durante el tiempo en que el usuario mantenga su tarjeta activa, y hasta 3 años después de la última actividad, salvo obligación legal de conservación mayor.</p>

<h2>6. Derechos del usuario</h2>
<p>Puede ejercer sus derechos de acceso, rectificación, supresión, limitación, portabilidad y oposición contactando con el negocio emisor de su tarjeta, o con ZubCard a través de: <a href="mailto:hola@zubcard.com">hola@zubcard.com</a>.</p>

<h2>7. Transferencias internacionales</h2>
<p>Los datos son alojados en servidores dentro del Espacio Económico Europeo o en proveedores con garantías adecuadas conforme al RGPD.</p>

<h2>8. Modificaciones</h2>
<p>Esta política puede actualizarse. La versión vigente siempre estará disponible en esta página.</p>
"""

TERMS_CONTENT = """
<h2>1. Objeto</h2>
<p>Los presentes Términos de Uso regulan el acceso y uso de la plataforma ZubCard, tanto por parte de los negocios que contratan el servicio (en adelante, "Negocios") como por los usuarios finales de los programas de fidelización.</p>

<h2>2. Registro y acceso</h2>
<p>Para acceder como Negocio es necesario crear una cuenta con datos verídicos. El usuario es responsable de mantener la confidencialidad de su PIN de administración. ZubCard no será responsable de los accesos no autorizados derivados del incumplimiento de esta obligación.</p>

<h2>3. Uso permitido</h2>
<p>El servicio está destinado exclusivamente a la gestión de programas de fidelización legales y legítimos. Queda expresamente prohibido:</p>
<ul>
  <li>Utilizar la plataforma para actividades ilegales o fraudulentas</li>
  <li>Intentar acceder a datos de otros negocios</li>
  <li>Realizar ingeniería inversa o copiar el software</li>
  <li>Enviar comunicaciones no solicitadas (spam)</li>
</ul>

<h2>4. Responsabilidad del Negocio</h2>
<p>Cada Negocio es responsable del contenido que publica en su programa de fidelización, del cumplimiento de la normativa de protección de datos aplicable respecto a sus clientes, y del uso adecuado de las funcionalidades de comunicación.</p>

<h2>5. Disponibilidad del servicio</h2>
<p>ZubCard se compromete a mantener el servicio disponible con un objetivo de uptime del 99,5%, sin garantizar disponibilidad ininterrumpida. Se realizarán mantenimientos con previo aviso cuando sea posible.</p>

<h2>6. Planes y facturación</h2>
<p>Los precios y condiciones de cada plan están disponibles en la página de precios. La facturación es mensual y el Negocio puede cancelar en cualquier momento, sin permanencia.</p>

<h2>7. Propiedad intelectual</h2>
<p>El código, diseño y marca ZubCard son propiedad exclusiva de sus desarrolladores. Los contenidos creados por cada Negocio (nombre, logotipo, textos) son propiedad del Negocio correspondiente.</p>

<h2>8. Ley aplicable</h2>
<p>Estos términos se rigen por la legislación española. Cualquier controversia se someterá a los juzgados y tribunales de la ciudad de Madrid.</p>
"""

COOKIES_CONTENT = """
<h2>¿Qué son las cookies?</h2>
<p>Las cookies son pequeños archivos de texto que los sitios web almacenan en tu dispositivo para recordar información sobre tu visita.</p>

<h2>Cookies que utilizamos</h2>

<h2>Cookies estrictamente necesarias</h2>
<p>Son imprescindibles para el funcionamiento básico del sitio. No pueden desactivarse.</p>
<ul>
  <li><strong>session</strong> – Mantiene tu sesión autenticada como administrador. Duración: sesión.</li>
</ul>

<h2>Cookies funcionales</h2>
<p>Mejoran la experiencia pero no son estrictamente necesarias.</p>
<ul>
  <li><strong>zubcard_pin_*</strong> – Almacena temporalmente el PIN para el inicio de sesión automático tras el registro. Se elimina al usarse (sessionStorage, no persistente).</li>
</ul>

<h2>Cookies analíticas</h2>
<p>Actualmente no utilizamos cookies de análisis o seguimiento de terceros.</p>

<h2>Cookies de marketing</h2>
<p>No utilizamos cookies de publicidad o retargeting.</p>

<h2>¿Cómo gestionar las cookies?</h2>
<p>Puedes configurar tu navegador para bloquear o eliminar las cookies. Ten en cuenta que desactivar las cookies necesarias puede afectar al funcionamiento del sitio.</p>
<ul>
  <li><a href="https://support.google.com/chrome/answer/95647" target="_blank" rel="noopener">Google Chrome</a></li>
  <li><a href="https://support.mozilla.org/es/kb/habilitar-y-deshabilitar-cookies-sitios-web-rastrear-preferencias" target="_blank" rel="noopener">Mozilla Firefox</a></li>
  <li><a href="https://support.apple.com/es-es/guide/safari/sfri11471/mac" target="_blank" rel="noopener">Safari</a></li>
</ul>

<h2>Actualizaciones</h2>
<p>Esta política de cookies puede actualizarse para reflejar cambios en el servicio. La fecha de última actualización aparece al inicio de esta página.</p>
"""

@app.get("/privacidad", response_class=HTMLResponse)
def privacy_page(request: Request):
    return templates.TemplateResponse("legal.html", {
        "request": request,
        "page_title": "Política de Privacidad",
        "page_id": "privacidad",
        "content": PRIVACY_CONTENT,
    })

@app.get("/terminos", response_class=HTMLResponse)
def terms_page(request: Request):
    return templates.TemplateResponse("legal.html", {
        "request": request,
        "page_title": "Términos de Uso",
        "page_id": "terminos",
        "content": TERMS_CONTENT,
    })

@app.get("/cookies", response_class=HTMLResponse)
def cookies_page(request: Request):
    return templates.TemplateResponse("legal.html", {
        "request": request,
        "page_title": "Política de Cookies",
        "page_id": "cookies",
        "content": COOKIES_CONTENT,
    })


# ══════════════════════════════════════════════════════════════════════════════
# TARJETA PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/card/{card_id}", response_class=HTMLResponse)
def show_card(card_id: str, request: Request, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    first_name = customer.first_name if customer else "Cliente"

    # Load business-scoped card_title for the page header
    card_title = CARD_TITLE  # global fallback
    if customer and customer.business_id:
        row = db.execute(
            text("SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"),
            {"bid": str(customer.business_id)}
        ).fetchone()
        if row:
            cfg = json.loads(row[0]) if row[0] else {}
            card_title = cfg.get("general", {}).get("card_title") or \
                         cfg.get("general", {}).get("card_name") or card_title

    return templates.TemplateResponse("card.html", {
        "request":           request,
        "card_id":           card_id,
        "first_name":        first_name,
        "name":              first_name,
        "card_title":        card_title,
        "api_base":          BASE_URL,
        "stamps":            card.stamps or 0,
        "stamps_per_reward": STAMPS_PER_REWARD,
        "rewards_redeemed":  card.rewards_redeemed or 0,
        "award_balance":     card.award_balance or 0,
        "total_stamps":      card.total_stamps or 0,
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

    # Resolve business for multi-tenant registration
    slug = str(body.get("slug") or "").strip()
    biz = get_business_by_slug(slug, db) if slug else None
    biz_id = biz.id if biz else None

    # Check if already registered within this business
    existing_q = db.query(models.Customer).filter(models.Customer.email == email)
    if biz_id:
        existing_q = existing_q.filter(models.Customer.business_id == biz_id)
    existing = existing_q.first()
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
        business_id  = biz_id,
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(pin, db)
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
    slug: str = "",
    db: Session = Depends(get_db),
):
    verify_pin(pin, db)
    q = (db.query(models.LoyaltyCard, models.Customer)
         .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
         .filter(models.Customer.email != "PLACEHOLDER@sukie.internal"))

    # If slug provided, filter by that business's customers
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            q = q.filter(models.Customer.business_id == biz.id)

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
    verify_pin(pin, db)
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return card_to_dict(card, customer)


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: CREAR CLIENTE
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/customers")
async def create_customer_admin(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    # Resolve business for multi-tenant
    slug = str(body.get("slug", "")).strip()
    biz = get_business_by_slug(slug, db) if slug else None
    biz_id = biz.id if biz else None

    # Check uniqueness within this business (not globally)
    existing_q = db.query(models.Customer).filter(models.Customer.email == email)
    if biz_id:
        existing_q = existing_q.filter(models.Customer.business_id == biz_id)
    if existing_q.first():
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
        origin      = body.get("origin", "Admin"),
        channel     = "Manual",
        business_id = biz_id,
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
    verify_pin(str(body.get("pin", "")), db)
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
async def delete_customer(card_id: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    # Accept pin as query param OR in body
    if not pin:
        try:
            body = await request.json()
            pin = str(body.get("pin", ""))
        except Exception:
            pin = ""
    verify_pin(pin, db)
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
def search_customer(pin: str = "", q: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Mínimo 2 caracteres")
    like = f"%{q}%"
    query = (db.query(models.LoyaltyCard, models.Customer)
             .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
             .filter(or_(
                 models.Customer.email.ilike(like),
                 models.Customer.first_name.ilike(like),
                 models.Customer.last_name.ilike(like),
                 models.Customer.phone.ilike(like),
             )))

    # If slug provided, filter by that business's customers
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            query = query.filter(models.Customer.business_id == biz.id)

    rows = query.limit(20).all()
    return {"results": [card_to_dict(c, cu) for c, cu in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ESTADÍSTICAS GLOBALES
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/stats")
def admin_stats(pin: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    q = (db.query(models.LoyaltyCard, models.Customer)
         .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
         .filter(models.Customer.email != "PLACEHOLDER@sukie.internal"))

    # If slug provided, filter by that business's customers
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            q = q.filter(models.Customer.business_id == biz.id)

    rows = q.all()

    total       = len(rows)
    active      = sum(1 for _, c in rows if c.card_active is not False)
    t_stamps    = sum((card.total_stamps or 0) for card, _ in rows)
    t_balance   = sum((card.award_balance or 0) for card, _ in rows)
    t_redeem    = sum((card.rewards_redeemed or 0) for card, _ in rows)

    now = datetime.now(timezone.utc)
    cutoff30  = now - timedelta(days=30)
    cutoff60  = now - timedelta(days=60)

    # Get card IDs for this business to filter transactions
    biz_card_ids = [card.id for card, _ in rows]

    txs_q30 = db.query(models.StampTransaction).filter(
        models.StampTransaction.created_at >= cutoff30)
    txs_q_prev = db.query(models.StampTransaction).filter(
        models.StampTransaction.created_at >= cutoff60,
        models.StampTransaction.created_at < cutoff30)

    # Filter by business cards when slug provided
    if slug and biz_card_ids:
        txs_q30   = txs_q30.filter(models.StampTransaction.card_id.in_(biz_card_ids))
        txs_q_prev = txs_q_prev.filter(models.StampTransaction.card_id.in_(biz_card_ids))

    txs30    = txs_q30.all()
    txs_prev = txs_q_prev.all()

    stamps_30d  = sum(t.stamps_added for t in txs30 if (t.stamps_added or 0) > 0)
    redeems_30d = sum(1 for t in txs30 if t.transaction_type == "redeem")
    stamps_prev = sum(t.stamps_added for t in txs_prev if (t.stamps_added or 0) > 0)
    redeems_prev= sum(1 for t in txs_prev if t.transaction_type == "redeem")

    new_30d  = sum(1 for _, c in rows if c.created_at and c.created_at >= cutoff30)
    new_prev = sum(1 for _, c in rows if c.created_at and cutoff60 <= c.created_at < cutoff30)

    # Per-business stamps_per_reward
    biz_stamps_per_reward = biz.stamps_per_reward if (slug and biz) else STAMPS_PER_REWARD
    awards_issued_30d = stamps_30d // max(biz_stamps_per_reward, 1)

    return {
        "total_customers":    total,
        "active_cards":       active,
        "total_stamps":       t_stamps,
        "award_balance":      t_balance,
        "total_redeemed":     t_redeem,
        "stamps_per_reward":  biz_stamps_per_reward,
        "stamps_last_30d":    stamps_30d,
        "redeems_last_30d":   redeems_30d,
        "stamps_prev_30d":    stamps_prev,
        "redeems_prev_30d":   redeems_prev,
        "new_clients_30d":    new_30d,
        "new_clients_prev":   new_prev,
        "awards_issued_30d":  awards_issued_30d,
    }


# ══════════════════════════════════════════════════════════════════════════════
# BUSINESS-SPECIFIC PIN VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/biz/{slug}/verify-pin")
def verify_business_pin(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Verify PIN — accepts admin PIN or any active store employee PIN"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    # Admin PIN check
    if str(pin) == str(biz.admin_pin):
        return {"status": "ok", "business": biz.name, "role": "admin"}
    # Store employee PIN check
    try:
        store = db.execute(text(
            "SELECT name FROM stores WHERE business_id=:bid AND pin=:pin AND active=TRUE LIMIT 1"
        ), {"bid": str(biz.id), "pin": str(pin)}).fetchone()
        if store:
            return {"status": "ok", "business": biz.name, "role": "employee", "store": store[0]}
    except Exception:
        pass
    raise HTTPException(status_code=403, detail="PIN incorrecto")


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ACTIVIDAD DIARIA (últimos N días)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/activity")
def admin_activity(pin: str = "", days: int = 30, slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    txs_q = (db.query(models.StampTransaction)
             .filter(models.StampTransaction.created_at >= cutoff))

    # Filter by business when slug provided
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            biz_card_ids = [c.id for c in db.query(models.LoyaltyCard)
                            .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
                            .filter(models.Customer.business_id == biz.id).all()]
            if biz_card_ids:
                txs_q = txs_q.filter(models.StampTransaction.card_id.in_(biz_card_ids))

    txs = txs_q.order_by(models.StampTransaction.created_at.asc()).all()

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
def admin_birthdays(pin: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    today = datetime.now().strftime("%m-%d")
    this_month = datetime.now().strftime("%m")

    query = (db.query(models.Customer, models.LoyaltyCard)
             .join(models.LoyaltyCard, models.LoyaltyCard.customer_id == models.Customer.id)
             .filter(models.Customer.birth_date.isnot(None),
                     models.Customer.birth_date != ""))

    # If slug provided, filter by that business's customers
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            query = query.filter(models.Customer.business_id == biz.id)

    all_custs = query.all()

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
def top_customers(pin: str = "", limit: int = 10, slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    q = (db.query(models.LoyaltyCard, models.Customer)
         .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
         .filter(models.Customer.email != "PLACEHOLDER@sukie.internal"))

    # If slug provided, filter by that business's customers
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            q = q.filter(models.Customer.business_id == biz.id)

    rows = q.order_by(models.LoyaltyCard.total_stamps.desc()).limit(limit).all()
    return {"top": [card_to_dict(c, cu) for c, cu in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: IMPORTAR CSV
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/import-csv")
async def import_csv(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    customers_data = body.get("customers", [])
    slug = body.get("slug", "")

    # Resolve business for multi-tenant import
    biz = get_business_by_slug(slug, db) if slug else None
    biz_id = biz.id if biz else None
    stamps_per_reward = biz.stamps_per_reward if biz else STAMPS_PER_REWARD

    created = 0; skipped = 0; errors = []
    for row in customers_data:
        email = (row.get("email") or "").strip().lower()
        if not email:
            errors.append(f"Fila sin email: {row}")
            continue
        # Check uniqueness within the business (not globally)
        existing_q = db.query(models.Customer).filter(models.Customer.email == email)
        if biz_id:
            existing_q = existing_q.filter(models.Customer.business_id == biz_id)
        if existing_q.first():
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
                business_id  = biz_id,
            )
            db.add(cust)
            db.flush()
            stamps_n = int(row.get("stamps") or row.get("sellos") or 0)
            card = models.LoyaltyCard(customer_id=cust.id, stamps=stamps_n % stamps_per_reward,
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
def export_csv(pin: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    q = (db.query(models.LoyaltyCard, models.Customer)
         .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
         .filter(models.Customer.email != "PLACEHOLDER@sukie.internal"))
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            q = q.filter(models.Customer.business_id == biz.id)
    rows = q.order_by(models.Customer.created_at.desc()).all()
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
        "card_title": "Tarjeta de Fidelización",
        "issuer": "",
        "description": "Tarjeta de sellos",
        "expiry_type": "never",  # never | date | days_from_register
        "expiry_date": "",
        "expiry_days": 365,
        "card_prefix": "C521",
        "barcode_type": "QR",
    },
    "programa": {
        "stamps_per_reward": 10,
        "reward_name": "Premio",
        "msg_single": "{nombre} has conseguido 1 sello",
        "msg_multiple": "{nombre} has conseguido {#} sellos",
        "birthday_enabled": True,
        "birthday_msg_push": "¡Feliz Cumpleaños! 🎂",
        "birthday_time": "07:00",
        "anniversary_enabled": True,
    },
    "landing": {
        "form_title": "Únete a nuestro programa",
        "header_text": "Regístrate y acumula puntos",
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
        "company_name": "",
        "company_email": "",
        "company_phone": "",
        "terms_url": "",
        "register_limit_date": "",
        "access_type": "public",
    },
    "diseno": {
        "commercial_name": "",
        "card_bg_color": "#FFFFC6",
        "label_color": "#220015",
        "text_color": "#220015",
        "stamp_bg_color": "#22000F",
        "stamp_icon_color": "#FFF5B6",
        "stamp_border_color": "#FFF5B6",
        "stamp_filled_color": "#FF6B9D",
        "front_field1_label": "Titular",
        "front_field1_value": "NOMBRE Y APELLIDOS",
        "front_field2_label": "Premios disponibles",
        "front_field2_value": "Premios/Vales Disponibles",
        "link_instagram": "",
        "link_web": "",
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
        "welcome_email_subject": "¡Bienvenido/a! 🎉",
        "welcome_email_body": "Hola {nombre},\n\nYa eres parte de nuestro programa de fidelización.\n\nVer tu tarjeta: {link_tarjeta}\n\n¡Hasta pronto!",
        "birthday_email_enabled": True,
        "birthday_email_subject": "¡Feliz Cumpleaños! 🎂",
        "birthday_email_body": "Hola {nombre},\n\n¡Hoy es tu día especial!\nPasa a visitarnos y llévate un regalo.\n\nCon cariño,\n{nombre_negocio}",
    },
    "club": {
        "nombre": "Club VIP",
        "descripcion": "Los mejores clientes merecen los mejores premios",
        "tagline": "Sé parte de algo especial",
        "activo": True
    },
    "tiers": [
        {
            "nombre": "Bronce",
            "emoji": "🥉",
            "color": "#CD7F32",
            "bg_color": "#FFF3E6",
            "min_sellos_totales": 0,
            "sellos_por_premio": 10,
            "premio_nombre": "Cookie gratis",
            "premio_descripcion": "1 cookie de tu elección",
            "premio_emoji": "🍪",
            "beneficios": [
                "1 cookie gratis cada 10 sellos",
                "Sorpresa especial en tu cumpleaños",
                "Acceso a ofertas exclusivas del club"
            ]
        },
        {
            "nombre": "Plata",
            "emoji": "🥈",
            "color": "#A8A8B3",
            "bg_color": "#F5F5FF",
            "min_sellos_totales": 50,
            "sellos_por_premio": 8,
            "premio_nombre": "Box de 6 cookies",
            "premio_descripcion": "Elige 6 cookies de la vitrina",
            "premio_emoji": "📦",
            "beneficios": [
                "Box 6 cookies cada 8 sellos",
                "10% descuento en todas tus compras",
                "Acceso anticipado a nuevas recetas",
                "Doble sorpresa de cumpleaños",
                "Badge exclusivo de miembro Plata"
            ]
        },
        {
            "nombre": "Oro",
            "emoji": "👑",
            "color": "#c8a84b",
            "bg_color": "#FFFBF0",
            "min_sellos_totales": 150,
            "sellos_por_premio": 7,
            "premio_nombre": "Box premium + bebida",
            "premio_descripcion": "Box 12 cookies premium + bebida gratis a elegir",
            "premio_emoji": "✨",
            "beneficios": [
                "Box premium + bebida cada 7 sellos",
                "20% descuento permanente",
                "Pedidos especiales y personalizados",
                "Acceso VIP a eventos exclusivos",
                "Regalo de aniversario como miembro",
                "Línea directa WhatsApp prioritaria"
            ]
        }
    ]
}

@app.get("/api/admin/config")
def get_config(pin: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    # Try to find config by business slug first, fall back to id=1
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            row = db.execute(text(
                "SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"
            ), {"bid": str(biz.id)}).fetchone()
            if not row:
                # Create config row for this business if missing
                db.execute(text(
                    "INSERT INTO card_config (config, business_id, updated_at) VALUES ('{}', :bid, NOW()) ON CONFLICT DO NOTHING"
                ), {"bid": str(biz.id)})
                db.commit()
                row = db.execute(text(
                    "SELECT config FROM card_config WHERE business_id=:bid LIMIT 1"
                ), {"bid": str(biz.id)}).fetchone()
        else:
            row = db.execute(text("SELECT config FROM card_config WHERE id=1")).fetchone()
    else:
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
    # Fill in business-specific defaults from the actual business record
    # so new tenants don't see placeholder or another tenant's name
    if biz:
        diseno = dict(result.get("diseno", {}))
        if not diseno.get("commercial_name"):
            diseno["commercial_name"] = biz.name
        result["diseno"] = diseno

        general = dict(result.get("general", {}))
        if not general.get("issuer"):
            general["issuer"] = biz.name
        if not general.get("description") or general.get("description") == "Tarjeta de sellos":
            general["description"] = f"Tarjeta de sellos {biz.name}"
        result["general"] = general

        landing = dict(result.get("landing", {}))
        if not landing.get("company_email") and hasattr(biz, "email") and biz.email:
            landing["company_email"] = biz.email
        if landing.get("form_title") == "Únete a nuestro programa":
            landing["form_title"] = f"Únete a {biz.name}"
        result["landing"] = landing

    return result


@app.put("/api/admin/config")
async def save_config(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    slug = str(body.get("slug", "")).strip()
    data = {k: v for k, v in body.items() if k not in ("pin", "slug")}
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            exists = db.execute(text(
                "SELECT 1 FROM card_config WHERE business_id=:bid LIMIT 1"
            ), {"bid": str(biz.id)}).fetchone()
            if exists:
                db.execute(text(
                    "UPDATE card_config SET config=:cfg, updated_at=NOW() WHERE business_id=:bid"
                ), {"cfg": json.dumps(data), "bid": str(biz.id)})
            else:
                db.execute(text(
                    "INSERT INTO card_config (config, business_id, updated_at) VALUES (:cfg, :bid, NOW())"
                ), {"cfg": json.dumps(data), "bid": str(biz.id)})
            db.commit()
            return {"message": "Configuración guardada"}
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(pin, db)
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
    verify_pin(str(body.get("pin", "")), db)
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
    verify_pin(pin, db)
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


@app.get("/api/cards/{card_id}/info")
async def get_card_info(card_id: str, db: Session = Depends(get_db)):
    """Returns basic card info for the card page"""
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    return {
        "card_id": str(card.id),
        "stamps": card.stamps,
        "total_stamps": card.total_stamps,
        "rewards_redeemed": card.rewards_redeemed,
        "award_balance": card.award_balance,
        "first_name": customer.first_name if customer else "",
        "email": customer.email if customer else "",
    }


@app.get("/api/cards/{card_id}/club-info")
async def get_club_info(card_id: str, db: Session = Depends(get_db)):
    """Returns full club/tier config for a card"""
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    # ── Multi-tenant: load config scoped to this card's business ─────────────
    biz_id = customer.business_id if customer else None
    biz = db.query(models.Business).filter(models.Business.id == biz_id).first() if biz_id else None

    config = {}
    if biz_id:
        row = db.execute(
            text("SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"),
            {"bid": str(biz_id)}
        ).fetchone()
        if row:
            config = json.loads(row[0]) if row[0] else {}
    else:
        # Legacy fallback — row id=1 (Sukie Cookie or first business)
        config_row = db.query(models.CardConfig).first()
        config = json.loads(config_row.config) if config_row else {}

    # ── Merge with defaults, then apply biz-specific overrides ───────────────
    result = {}
    for section, defaults in DEFAULT_CONFIG.items():
        stored = config.get(section, {})
        merged = {**defaults, **{k: v for k, v in stored.items() if v not in (None, "")}}
        result[section] = merged

    # Dynamic biz-name override (mirrors get_config logic)
    if biz:
        club_sec = dict(result.get("club", {}))
        if not config.get("club", {}).get("nombre"):
            club_sec["nombre"] = f"Club VIP {biz.name}"
        result["club"] = club_sec

    # Merge with defaults
    default_tiers = DEFAULT_CONFIG.get("tiers", [])
    tiers = config.get("tiers", default_tiers)
    club = result.get("club", DEFAULT_CONFIG.get("club", {}))

    total = card.total_stamps or 0
    current_tier = tiers[0] if tiers else {}
    next_tier = None

    for i, t in enumerate(tiers):
        if total >= t.get("min_sellos_totales", 0):
            current_tier = t
            next_tier = tiers[i+1] if i+1 < len(tiers) else None

    stamps_in_tier_cycle = card.stamps  # current stamps on card
    stamps_per_reward = current_tier.get("sellos_por_premio", 10)
    stamps_to_next_reward = max(0, stamps_per_reward - stamps_in_tier_cycle)

    stamps_to_next_tier = None
    if next_tier:
        stamps_to_next_tier = max(0, next_tier.get("min_sellos_totales", 0) - total)

    # ── Member number: scoped to this business ────────────────────────────────
    if customer:
        member_q = db.query(models.Customer).filter(
            models.Customer.created_at <= customer.created_at
        )
        if biz_id:
            member_q = member_q.filter(models.Customer.business_id == biz_id)
        member_number = member_q.count()
    else:
        member_number = 1

    return {
        "club": club,
        "current_tier": current_tier,
        "next_tier": next_tier,
        "total_stamps": total,
        "stamps_on_card": stamps_in_tier_cycle,
        "stamps_to_next_reward": stamps_to_next_reward,
        "stamps_to_next_tier": stamps_to_next_tier,
        "award_balance": card.award_balance,
        "member_number": member_number,
        "member_since": customer.created_at.strftime("%B %Y") if customer and customer.created_at else "",
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


@app.get("/login", response_class=HTMLResponse)
async def biz_login_page(request: Request):
    """General business owner login page — enter slug + PIN → redirect to dashboard"""
    return templates.TemplateResponse("login.html", {"request": request})


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
    
    # Create their default card config (id uses auto-increment sequence)
    db.execute(text(
        "INSERT INTO card_config (config, business_id, updated_at) VALUES ('{}', :bid, NOW()) ON CONFLICT DO NOTHING"
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


# ════════════════════════════════════════════════════════════════════════════════
# BUSINESS PROFILE — GET / PUT / CHANGE-PIN
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/biz/{slug}/profile")
def get_biz_profile(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Get business profile info (name, email, slug, industry)"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    return {
        "name":     biz.name,
        "email":    biz.email,
        "slug":     biz.slug,
        "industry": getattr(biz, "industry", "other"),
        "plan":     getattr(biz, "plan", "pro"),
        "created_at": str(biz.created_at) if hasattr(biz, "created_at") else None,
    }


@app.put("/api/biz/{slug}/profile")
async def update_biz_profile(slug: str, request: Request, db: Session = Depends(get_db)):
    """Update business name and industry"""
    body = await request.json()
    pin  = str(body.get("pin", "")).strip()
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    name     = (body.get("name") or "").strip()
    industry = (body.get("industry") or "other").strip()
    if name:
        db.execute(text("UPDATE businesses SET name=:name WHERE slug=:slug"), {"name": name, "slug": slug})
    db.execute(text("UPDATE businesses SET industry=:ind WHERE slug=:slug"), {"ind": industry, "slug": slug})
    db.commit()
    return {"status": "updated", "name": name or biz.name, "industry": industry}


@app.post("/api/biz/{slug}/change-pin")
async def change_biz_pin(slug: str, request: Request, db: Session = Depends(get_db)):
    """Change admin PIN"""
    body        = await request.json()
    current_pin = str(body.get("current_pin", "")).strip()
    new_pin     = str(body.get("new_pin", "")).strip()
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if current_pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN actual incorrecto")
    if len(new_pin) < 4:
        raise HTTPException(status_code=400, detail="El nuevo PIN debe tener al menos 4 dígitos")
    db.execute(text("UPDATE businesses SET admin_pin=:pin WHERE slug=:slug"), {"pin": new_pin, "slug": slug})
    db.commit()
    return {"status": "updated"}


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
    # Load landing config for this business
    landing_cfg = {}
    try:
        row = db.execute(text(
            "SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"
        ), {"bid": str(biz.id)}).fetchone()
        if not row:
            row = db.execute(text("SELECT config FROM card_config WHERE id=1")).fetchone()
        if row:
            stored = json.loads(row[0])
            landing_cfg = stored.get("landing", {})
    except Exception:
        pass
    return templates.TemplateResponse("register.html", {
        "request":           request,
        "card_title":        biz.card_title or biz.name,
        "biz_name":          biz.name,
        "logo_url":          biz.logo_url or "",
        "primary_color":     biz.primary_color or "#00e676",
        "accent_color":      biz.accent_color or "#a8f0d0",
        "api_base":          BASE_URL,
        "stamps_per_reward": biz.stamps_per_reward or STAMPS_PER_REWARD,
        "biz_slug":          slug,
        "ref":               ref,
        # Landing customization from config
        "form_title":        landing_cfg.get("form_title", ""),
        "header_text":       landing_cfg.get("header_text", ""),
        "button_text":       landing_cfg.get("button_text", ""),
    })


@app.get("/biz/{slug}/card/{card_id}", response_class=HTMLResponse)
async def biz_card(slug: str, card_id: str, request: Request, db: Session = Depends(get_db)):
    """Business-specific customer card page"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    first_name = customer.first_name if customer else "Cliente"
    card_url = f"{BASE_URL}/biz/{slug}/card/{card_id}"
    primary_color = biz.primary_color or "#3A3426"
    # Generate Google Wallet URL (None if not configured)
    google_wallet_url = generate_google_wallet_url(
        card_id=card_id,
        biz_slug=slug,
        biz_name=biz.name,
        customer_name=first_name,
        stamps=card.stamps or 0,
        stamps_per_reward=biz.stamps_per_reward or STAMPS_PER_REWARD,
        card_url=card_url,
        primary_color=primary_color,
    )
    return templates.TemplateResponse("card.html", {
        "request":            request,
        "card_id":            card_id,
        "first_name":         first_name,
        "stamps":             card.stamps or 0,
        "stamps_per_reward":  biz.stamps_per_reward,
        "rewards_redeemed":   card.rewards_redeemed or 0,
        "award_balance":      card.award_balance or 0,
        "total_stamps":       card.total_stamps or 0,
        "biz_name":           biz.name,
        "biz_slug":           slug,
        "card_title":         biz.card_title or biz.name,
        "primary_color":      primary_color,
        "accent_color":       biz.accent_color or "#a8f0d0",
        "api_base":           BASE_URL,
        "google_wallet_url":  google_wallet_url or "",
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


@app.get("/api/debug/db")
def debug_db(pin: str = "", db: Session = Depends(get_db)):
    """Temporary debug: raw SQL check of businesses table"""
    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    try:
        rows = db.execute(text("SELECT id, name, slug, email, admin_pin, plan, active FROM businesses")).fetchall()
        return {"count": len(rows), "rows": [dict(r._mapping) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/cleanup-test-data")
def cleanup_test_data(pin: str = "", db: Session = Depends(get_db)):
    """One-time cleanup: remove test/placeholder customers"""
    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    card_ids = [
        'b67ba1b0-f365-4547-8784-da4a2925ab6d',
        '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f',
        '76787185-18d7-4189-9bc1-a256f6f0ea6d'
    ]
    results = []
    for card_id in card_ids:
        try:
            cid = f"'{card_id}'::uuid"
            db.execute(text(f"DELETE FROM push_subscriptions WHERE card_id = {cid}"))
            db.execute(text(f"DELETE FROM referrals WHERE referrer_card = {cid} OR referred_card = {cid}"))
            db.execute(text(f"DELETE FROM stamp_transactions WHERE card_id = {cid}"))
            # Get customer_id before deleting card
            row = db.execute(text(f"SELECT customer_id FROM loyalty_cards WHERE id = {cid}")).fetchone()
            db.execute(text(f"DELETE FROM loyalty_cards WHERE id = {cid}"))
            if row:
                db.execute(text(f"DELETE FROM customers WHERE id = '{row[0]}'"))
            db.commit()
            results.append({"card_id": card_id, "status": "deleted"})
        except Exception as e:
            db.rollback()
            results.append({"card_id": card_id, "status": "error", "detail": str(e)})
    return {"results": results}


# ════════════════════════════════════════════════════════════════════════════════
# DELETE BUSINESS ACCOUNT
# ══════════════════════════════════════════════════════════════════════════════
@app.delete("/api/app/businesses/{slug}")
def delete_business(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """
    Permanently delete a business account and ALL its data.
    Requires the business admin PIN as query param: ?pin=XXXX
    Protected: cannot delete the seed business (sukiecookie).
    """
    if slug == "sukiecookie":
        raise HTTPException(status_code=403, detail="No puedes eliminar el negocio base.")

    biz = db.execute(
        text("SELECT id, admin_pin FROM businesses WHERE slug=:slug"),
        {"slug": slug}
    ).fetchone()
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    if pin != str(biz[1]):
        raise HTTPException(status_code=403, detail="PIN incorrecto")

    bid = str(biz[0])
    try:
        # Delete all related data in dependency order
        db.execute(text("DELETE FROM push_subscriptions WHERE card_id IN (SELECT id FROM loyalty_cards WHERE business_id=:bid)"), {"bid": bid})
        db.execute(text("DELETE FROM referrals WHERE referrer_card IN (SELECT id FROM loyalty_cards WHERE business_id=:bid) OR referred_card IN (SELECT id FROM loyalty_cards WHERE business_id=:bid)"), {"bid": bid})
        db.execute(text("DELETE FROM stamp_transactions WHERE card_id IN (SELECT id FROM loyalty_cards WHERE business_id=:bid)"), {"bid": bid})
        # Get customer IDs before deleting cards
        customer_ids = db.execute(text("SELECT customer_id FROM loyalty_cards WHERE business_id=:bid"), {"bid": bid}).fetchall()
        db.execute(text("DELETE FROM loyalty_cards WHERE business_id=:bid"), {"bid": bid})
        for row in customer_ids:
            db.execute(text("DELETE FROM customers WHERE id=:cid"), {"cid": str(row[0])})
        # Delete business-level data
        db.execute(text("DELETE FROM stores WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM campaigns WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM card_programs WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM custom_qrs WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM activity_log WHERE business_id=:bid"), {"bid": bid})
        # Finally delete the business itself
        db.execute(text("DELETE FROM businesses WHERE id=:bid"), {"bid": bid})
        db.commit()
        return {"status": "deleted", "slug": slug}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al eliminar: {str(e)}")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    return templates.TemplateResponse("dashboard_admin.html", {"request": request})


# ════════════════════════════════════════════════════════════════════════════════
# SCANNER — Página de caja para escanear QR y añadir sellos
# Acceso: /biz/{slug}/scanner
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/biz/{slug}/scanner", response_class=HTMLResponse)
async def scanner_page(request: Request, slug: str, db: Session = Depends(get_db)):
    """Página de caja: escanea QR del cliente y añade sellos por producto."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return templates.TemplateResponse("scanner.html", {
        "request":           request,
        "slug":              slug,
        "business_name":     biz.name,
        "primary_color":     biz.primary_color or "#3A3426",
        "accent_color":      biz.accent_color  or "#FFF3CF",
        "stamps_per_reward": biz.stamps_per_reward or STAMPS_PER_REWARD,
        "base_url":          BASE_URL,
    })


# ════════════════════════════════════════════════════════════════════════════════
# TIENDAS / LOCALES  — CRUD
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/stores")
def list_stores(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT id, name, pin, notes, active, created_at FROM stores WHERE business_id=:bid ORDER BY created_at ASC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"stores": [
        {"id": str(r[0]), "name": r[1], "pin": r[2], "notes": r[3] or "", "active": bool(r[4]), "created_at": str(r[5])}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/stores")
def create_store(slug: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    store_id = str(uuid.uuid4())
    db.execute(text(
        "INSERT INTO stores (id, business_id, name, pin, notes, active) VALUES (:id, :bid, :name, :pin, :notes, :active)"
    ), {"id": store_id, "bid": str(biz.id), "name": payload.get("name", ""),
        "pin": str(payload.get("pin", "")), "notes": payload.get("notes", ""),
        "active": payload.get("active", True)})
    db.commit()
    return {"id": store_id, "status": "created"}


@app.put("/api/biz/{slug}/stores/{store_id}")
def update_store(slug: str, store_id: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text(
        "UPDATE stores SET name=:name, pin=:pin, notes=:notes, active=:active WHERE id=:id AND business_id=:bid"
    ), {"name": payload.get("name", ""), "pin": str(payload.get("pin", "")),
        "notes": payload.get("notes", ""), "active": payload.get("active", True),
        "id": store_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "updated"}


@app.delete("/api/biz/{slug}/stores/{store_id}")
def delete_store(slug: str, store_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text("DELETE FROM stores WHERE id=:id AND business_id=:bid"),
               {"id": store_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "deleted"}


# ════════════════════════════════════════════════════════════════════════════════
# PASSCODES
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/passcodes")
def list_passcodes(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT p.id, p.code, p.stamps, p.used, p.expires_at, p.created_at, c.first_name, c.last_name "
        "FROM passcodes p "
        "LEFT JOIN loyalty_cards lc ON lc.id = p.used_by "
        "LEFT JOIN customers c ON c.id = lc.customer_id "
        "WHERE p.business_id=:bid ORDER BY p.created_at DESC LIMIT 500"
    ), {"bid": str(biz.id)}).fetchall()
    return {"codes": [
        {"id": str(r[0]), "code": r[1], "stamps": r[2], "used": bool(r[3]),
         "expires_at": str(r[4]) if r[4] else None, "created_at": str(r[5]),
         "used_by_name": f"{r[6] or ''} {r[7] or ''}".strip() if r[3] else None}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/passcodes/generate")
def generate_passcodes(slug: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    count = min(int(payload.get("count", 10)), 500)
    stamps = int(payload.get("stamps_per_code", 1))
    expires_at = payload.get("expires_at") or None
    generated = 0
    for _ in range(count):
        for _attempt in range(10):
            code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
            existing = db.execute(text("SELECT 1 FROM passcodes WHERE code=:code"), {"code": code}).fetchone()
            if not existing:
                db.execute(text(
                    "INSERT INTO passcodes (id, business_id, code, stamps, expires_at) "
                    "VALUES (:id, :bid, :code, :stamps, :exp)"
                ), {"id": str(uuid.uuid4()), "bid": str(biz.id), "code": code,
                    "stamps": stamps, "exp": expires_at})
                generated += 1
                break
    db.commit()
    return {"generated": generated}


@app.post("/api/biz/{slug}/passcodes/redeem")
def redeem_passcode(slug: str, payload: dict = Body(...), db: Session = Depends(get_db)):
    """Public endpoint: customer redeems a passcode to get stamps on their card."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    code = (payload.get("code") or "").strip().upper()
    card_id = payload.get("card_id")
    if not code or not card_id:
        raise HTTPException(status_code=400, detail="Código y tarjeta requeridos")
    row = db.execute(text(
        "SELECT id, stamps, used, expires_at FROM passcodes WHERE code=:code AND business_id=:bid"
    ), {"code": code, "bid": str(biz.id)}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Código no encontrado")
    if row[2]:
        raise HTTPException(status_code=409, detail="Este código ya fue usado")
    if row[3] and row[3] < datetime.now(timezone.utc):
        raise HTTPException(status_code=410, detail="Código caducado")
    stamps = row[1]
    db.execute(text(
        "UPDATE passcodes SET used=TRUE, used_by=:cid, used_at=NOW() WHERE id=:id"
    ), {"cid": card_id, "id": str(row[0])})
    db.execute(text(
        "UPDATE loyalty_cards SET stamp_count = stamp_count + :s WHERE id=:cid"
    ), {"s": stamps, "cid": card_id})
    db.execute(text(
        "INSERT INTO stamp_transactions (id, card_id, stamps_added, transaction_type, note, created_at) "
        "VALUES (:id, :cid, :s, 'passcode', :note, NOW())"
    ), {"id": str(uuid.uuid4()), "cid": card_id, "s": stamps, "note": f"PassCode: {code}"})
    db.commit()
    return {"status": "ok", "stamps_added": stamps}


# ════════════════════════════════════════════════════════════════════════════════
# CAMPAÑAS
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/campaigns")
def list_campaigns(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT id, name, subject, type, status, segment, created_at, sent_at "
        "FROM campaigns WHERE business_id=:bid ORDER BY created_at DESC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"campaigns": [
        {"id": str(r[0]), "name": r[1], "subject": r[2] or "", "type": r[3] or "email",
         "status": r[4] or "draft", "segment": r[5] or "all",
         "created_at": str(r[6]), "sent_at": str(r[7]) if r[7] else None}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/campaigns")
def create_campaign(slug: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    camp_id = str(uuid.uuid4())
    db.execute(text(
        "INSERT INTO campaigns (id, business_id, name, subject, body, type, status, segment) "
        "VALUES (:id, :bid, :name, :subject, :body, :type, :status, :segment)"
    ), {"id": camp_id, "bid": str(biz.id), "name": payload.get("name", ""),
        "subject": payload.get("subject", ""), "body": payload.get("body", ""),
        "type": payload.get("type", "email"), "status": payload.get("status", "draft"),
        "segment": payload.get("segment", "all")})
    db.commit()
    return {"id": camp_id, "status": "created"}


@app.post("/api/biz/{slug}/campaigns/{campaign_id}/send")
def send_campaign(slug: str, campaign_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Send email campaign to segmented customers"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    camp = db.execute(text(
        "SELECT name, subject, body, segment FROM campaigns WHERE id=:id AND business_id=:bid"
    ), {"id": campaign_id, "bid": str(biz.id)}).fetchone()
    if not camp:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")
    segment = camp[3] or "all"
    q_base = (
        "SELECT c.email, c.first_name FROM customers c "
        "JOIN loyalty_cards lc ON lc.customer_id=c.id "
        "WHERE c.business_id=:bid AND c.opt_in_email=TRUE AND c.email NOT LIKE '%placeholder%'"
    )
    if segment == "active":
        q_base += " AND lc.stamp_count > 0"
    customers = db.execute(text(q_base), {"bid": str(biz.id)}).fetchall()
    sent = 0
    for cust in customers:
        try:
            body_html = f"<p>{(camp[2] or '').replace('{nombre}', cust[1] or '')}</p>"
            subject_text = (camp[1] or "").replace("{nombre}", cust[1] or "")
            if send_email(cust[0], subject_text, body_html):
                sent += 1
        except Exception:
            pass
    db.execute(text(
        "UPDATE campaigns SET status='sent', sent_at=NOW() WHERE id=:id"
    ), {"id": campaign_id})
    db.commit()
    return {"sent": sent, "status": "sent"}


@app.delete("/api/biz/{slug}/campaigns/{campaign_id}")
def delete_campaign(slug: str, campaign_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text("DELETE FROM campaigns WHERE id=:id AND business_id=:bid"),
               {"id": campaign_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "deleted"}


# ════════════════════════════════════════════════════════════════════════════════
# CARD PROGRAMS (multi-tarjeta)
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/card-programs")
def list_card_programs(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT id, name, emoji, stamps_per_reward, reward_name, bg_color, accent_color, text_color, status, sort_order, created_at "
        "FROM card_programs WHERE business_id=:bid ORDER BY sort_order, created_at"
    ), {"bid": str(biz.id)}).fetchall()
    return [dict(r._mapping) for r in rows]

@app.post("/api/biz/{slug}/card-programs")
async def create_card_program(slug: str, request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    row = db.execute(text(
        "INSERT INTO card_programs (business_id, name, emoji, stamps_per_reward, reward_name, bg_color, accent_color, text_color, status) "
        "VALUES (:bid, :name, :emoji, :stamps, :reward, :bg, :accent, :txt, 'active') RETURNING id"
    ), {
        "bid":    str(biz.id),
        "name":   body.get("name", "Nueva Tarjeta"),
        "emoji":  body.get("emoji", "🃏"),
        "stamps": int(body.get("stamps_per_reward", 10)),
        "reward": body.get("reward_name", "Premio"),
        "bg":     body.get("bg_color", "#0a0a0a"),
        "accent": body.get("accent_color", "#00e676"),
        "txt":    body.get("text_color", "#ffffff"),
    }).fetchone()
    db.commit()
    return {"id": str(row[0]), "status": "created"}

@app.delete("/api/biz/{slug}/card-programs/{program_id}")
def delete_card_program(slug: str, program_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text("DELETE FROM card_programs WHERE id=:id AND business_id=:bid"),
               {"id": program_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "deleted"}


# ════════════════════════════════════════════════════════════════════════════════
# CUSTOM QRs DE ALTA
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/custom-qrs")
def list_custom_qrs(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT id, canal, local_name, created_at FROM custom_qrs WHERE business_id=:bid ORDER BY created_at ASC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"qrs": [
        {"id": str(r[0]), "canal": r[1], "local_name": r[2] or "", "created_at": str(r[3])}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/custom-qrs")
def create_custom_qr(slug: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    qr_id = str(uuid.uuid4())
    db.execute(text(
        "INSERT INTO custom_qrs (id, business_id, canal, local_name) VALUES (:id, :bid, :canal, :local)"
    ), {"id": qr_id, "bid": str(biz.id), "canal": payload.get("canal", ""),
        "local": payload.get("local_name", "")})
    db.commit()
    return {"id": qr_id, "status": "created"}


@app.delete("/api/biz/{slug}/custom-qrs/{qr_id}")
def delete_custom_qr(slug: str, qr_id: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text("DELETE FROM custom_qrs WHERE id=:id AND business_id=:bid"),
               {"id": qr_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "deleted"}


# ════════════════════════════════════════════════════════════════════════════════
# ACTIVITY LOG  — tabla de transacciones recientes (distinto al chart)
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/activity-log")
def activity_log(
    slug: str, pin: str = "", filter: str = "all",
    limit: int = 50, offset: int = 0,
    db: Session = Depends(get_db)
):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    bid = str(biz.id)

    # Summary stats for this business
    stats_row = db.execute(text(
        "SELECT COUNT(DISTINCT lc.id), "
        "COALESCE(SUM(CASE WHEN st.stamps_added > 0 THEN st.stamps_added ELSE 0 END), 0), "
        "COUNT(CASE WHEN st.transaction_type='redeem' THEN 1 END) "
        "FROM loyalty_cards lc "
        "JOIN customers c ON c.id = lc.customer_id "
        "LEFT JOIN stamp_transactions st ON st.card_id = lc.id "
        "WHERE c.business_id = :bid AND c.email NOT LIKE '%placeholder%'"
    ), {"bid": bid}).fetchone()

    stats = {
        "total_clients": int(stats_row[0]) if stats_row else 0,
        "total_stamps": int(stats_row[1]) if stats_row else 0,
        "total_redeemed": int(stats_row[2]) if stats_row else 0,
    }

    # Build type filter
    type_filter = ""
    if filter == "stamp":
        type_filter = " AND st.transaction_type IN ('stamp', 'passcode', 'adjust')"
    elif filter == "redeem":
        type_filter = " AND st.transaction_type = 'redeem'"
    elif filter == "register":
        type_filter = " AND st.transaction_type = 'register'"

    rows = db.execute(text(f"""
        SELECT st.created_at, c.first_name, c.last_name, c.email,
               st.transaction_type, st.stamps_added, st.note, st.store
        FROM stamp_transactions st
        JOIN loyalty_cards lc ON lc.id = st.card_id
        JOIN customers c ON c.id = lc.customer_id
        WHERE c.business_id = :bid
          AND c.email NOT LIKE '%placeholder%'
          {type_filter}
        ORDER BY st.created_at DESC
        LIMIT :limit OFFSET :offset
    """), {"bid": bid, "limit": limit, "offset": offset}).fetchall()

    return {
        "stats": stats,
        "rows": [
            {
                "created_at": r[0].isoformat() if r[0] else None,
                "name": f"{r[1] or ''} {r[2] or ''}".strip() or "—",
                "email": r[3] or "",
                "type": r[4] or "stamp",
                "amount": int(r[5] or 0),
                "note": r[6] or r[7] or "",
            }
            for r in rows
        ],
    }


# ════════════════════════════════════════════════════════════════════════════════
# GOOGLE OAUTH 2.0
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/auth/google")
async def auth_google_redirect(slug: str = ""):
    """Redirect user to Google's OAuth consent screen.
    Optionally pass ?slug=... to remember which business they're logging into."""
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "online",
        "prompt":        "select_account",
        "state":         slug,
    }
    google_auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return RedirectResponse(google_auth_url)


@app.get("/auth/google/callback")
async def auth_google_callback(
    code: str = "",
    state: str = "",
    error: str = "",
    db: Session = Depends(get_db),
):
    """Handle Google OAuth callback. Creates or logs in the business account."""
    if error or not code:
        return RedirectResponse(f"/app/login?error=google_cancelled")

    # Exchange code for tokens
    try:
        async with httpx.AsyncClient() as client:
            token_resp = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id":     GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "code":          code,
                    "redirect_uri":  GOOGLE_REDIRECT_URI,
                    "grant_type":    "authorization_code",
                },
            )
        tokens = token_resp.json()
        access_token = tokens.get("access_token")
        if not access_token:
            return RedirectResponse(f"/app/login?error=google_token_failed")

        # Fetch user profile
        async with httpx.AsyncClient() as client:
            userinfo_resp = await client.get(
                "https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        userinfo = userinfo_resp.json()
    except Exception:
        return RedirectResponse(f"/app/login?error=google_error")

    google_sub = userinfo.get("sub", "")
    email      = (userinfo.get("email") or "").lower().strip()
    full_name  = userinfo.get("name") or email.split("@")[0]

    if not email:
        return RedirectResponse(f"/app/login?error=no_email")

    # 1) Find by google_id first, then by email
    biz = db.query(models.Business).filter(models.Business.google_id == google_sub).first()
    if not biz:
        biz = db.query(models.Business).filter(models.Business.email == email).first()
        if biz:
            # Link google_id to existing account
            biz.google_id = google_sub
            db.commit()

    if not biz:
        # Create new business account
        base_slug = generate_slug(full_name)
        slug = base_slug
        counter = 1
        while db.query(models.Business).filter(models.Business.slug == slug).first():
            slug = f"{base_slug}{counter}"
            counter += 1

        random_pin = "".join(secrets.choice(string.digits) for _ in range(6))
        biz = models.Business(
            name      = full_name,
            slug      = slug,
            email     = email,
            google_id = google_sub,
            admin_pin = random_pin,
            api_key   = generate_api_key(),
            industry  = "other",
            plan      = "free",
        )
        db.add(biz)
        db.commit()
        db.refresh(biz)

        # Create default card_config (id uses auto-increment sequence)
        db.execute(text(
            "INSERT INTO card_config (config, business_id, updated_at) VALUES ('{}', :bid, NOW()) ON CONFLICT (business_id) DO NOTHING"
        ), {"bid": str(biz.id)})
        db.commit()

    # Redirect to dashboard (PIN passed in URL for session bootstrap)
    return RedirectResponse(f"/biz/{biz.slug}/dashboard?pin={biz.admin_pin}&google=1")
