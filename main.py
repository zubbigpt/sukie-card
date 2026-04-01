import os
import uuid
import csv
import bcrypt
import io
import json
import time
from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException, Request, Query, File, UploadFile, Body
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
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import models
from database import engine, get_db

# ── SIMPLE IN-MEMORY RATE LIMITER ─────────────────────────────────────────────
# Sliding window. Uses IP or card_id as key. Fine for single-replica deploys.
_rate_store: dict[str, list[float]] = defaultdict(list)

def check_rate_limit(key: str, max_requests: int = 5, window_seconds: int = 300) -> None:
    """Raise 429 if key has exceeded max_requests within window_seconds."""
    now = time.time()
    bucket = _rate_store[key]
    # Evict expired timestamps
    _rate_store[key] = [t for t in bucket if now - t < window_seconds]
    if len(_rate_store[key]) >= max_requests:
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes. Espera un momento.")
    _rate_store[key].append(now)

# ── CREAR TABLAS ──────────────────────────────────────────────────────────────
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="ZubCard API")
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
API_KEY           = os.environ.get("API_KEY", "zubcard-api-key")
CARD_TITLE        = os.environ.get("CARD_TITLE", "Tarjeta de Fidelización")
REWARD_NAME       = os.environ.get("REWARD_NAME", "Premio")

# SMTP CONFIG
SMTP_HOST     = os.environ.get("SMTP_HOST", "")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASS     = os.environ.get("SMTP_PASS", "")
SMTP_FROM     = os.environ.get("SMTP_FROM", "noreply@zubcard.com")
VAPID_PUBLIC  = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE = os.environ.get("VAPID_PRIVATE_KEY", "")

# GOOGLE OAUTH CONFIG
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", f"{os.environ.get('BASE_URL', 'https://zubcard.com')}/auth/google/callback")

# GOOGLE WALLET CONFIG
GOOGLE_WALLET_ISSUER_ID   = os.environ.get("GOOGLE_WALLET_ISSUER_ID", "")
GOOGLE_WALLET_CREDENTIALS = os.environ.get("GOOGLE_WALLET_CREDENTIALS", "")  # JSON string

# STRIPE CONFIG
STRIPE_SECRET_KEY      = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_PRICE_ID_PRO    = os.environ.get("STRIPE_PRICE_ID_PRO", "")       # price_xxx
STRIPE_WEBHOOK_SECRET  = os.environ.get("STRIPE_WEBHOOK_SECRET", "")      # whsec_xxx
STRIPE_PRO_PRICE_DISPLAY = os.environ.get("STRIPE_PRO_PRICE_DISPLAY", "€39")  # display only

# Inicializar Stripe SDK con la clave secreta
import stripe as _stripe_sdk
if STRIPE_SECRET_KEY:
    _stripe_sdk.api_key = STRIPE_SECRET_KEY

# PLAN LIMITS (Free tier)
FREE_LIMITS = {
    "max_customers":     100,
    "max_card_programs":   1,
    "campaigns":       False,   # cannot create/send campaigns
}

def _get_biz_plan(biz) -> str:
    return (getattr(biz, "plan", None) or "free").lower()

def _require_pro(biz):
    """Raise HTTP 402 if the business is on the Free plan."""
    if _get_biz_plan(biz) != "pro":
        raise HTTPException(status_code=402, detail="upgrade_required")

def generate_google_wallet_url(
    card_id: str,
    biz_slug: str,
    biz_name: str,
    customer_name: str,
    stamps: int,
    stamps_per_reward: int,
    card_url: str,
    primary_color: str = "#3A3426",
    accent_color: str = "#ffca48",
    reward_name: str = "Premio",
    logo_url: str = "",
    award_balance: int = 0,
    card_name: str = "",
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

        hex_bg     = primary_color if primary_color.startswith("#") else "#3A3426"
        stamps_left = max(0, stamps_per_reward - stamps)

        # Visual progress bar using filled/empty blocks
        filled  = min(stamps, stamps_per_reward)
        empty   = stamps_per_reward - filled
        bar     = "█" * filled + "░" * empty
        pct     = int((filled / stamps_per_reward) * 100) if stamps_per_reward else 0

        # Status line
        if award_balance > 0:
            status_val = f"Premio listo x{award_balance}" if award_balance > 1 else "Premio listo"
        elif stamps_left == 0:
            status_val = "Premio disponible"
        else:
            status_val = f"Faltan {stamps_left} sello{'s' if stamps_left != 1 else ''}"

        program_title = card_name or biz_name

        text_modules = [
            {
                "id": "progress_bar",
                "header": f"Sellos  {stamps}/{stamps_per_reward}  ({pct}%)",
                "body": bar,
            },
            {
                "id": "status",
                "header": "Estado",
                "body": status_val,
            },
            {
                "id": "reward_info",
                "header": "Premio",
                "body": reward_name,
            },
        ]
        if award_balance > 0:
            text_modules.append({
                "id": "awards",
                "header": "Premios disponibles",
                "body": str(award_balance),
            })

        generic_object = {
            "id": object_id,
            "classId": class_id,
            "genericType": "GENERIC_TYPE_UNSPECIFIED",
            "hexBackgroundColor": hex_bg,
            "cardTitle": {
                "defaultValue": {"language": "es", "value": program_title}
            },
            "subheader": {
                "defaultValue": {"language": "es", "value": "Tarjeta de Fidelidad"}
            },
            "header": {
                "defaultValue": {"language": "es", "value": customer_name}
            },
            "textModulesData": text_modules,
            "linksModuleData": {
                "uris": [
                    {
                        "uri": card_url,
                        "description": "Ver tarjeta web",
                        "id": "card_link",
                    }
                ]
            },
            "barcode": {
                "type": "QR_CODE",
                "value": card_url,
                "alternateText": card_id[:8].upper(),
            },
            "state": "ACTIVE",
        }

        # Add logo image if available
        if logo_url:
            generic_object["logo"] = {
                "sourceUri": {"uri": logo_url},
                "contentDescription": {"defaultValue": {"language": "es", "value": biz_name}},
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
        # ── Geo-location / address fields for push notifications ──────────────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS address VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS geo_radius_m INTEGER DEFAULT 300",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS geo_push_msg VARCHAR DEFAULT '¡Estás cerca! Visítanos y acumula sellos 🎉'",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS description VARCHAR",
        # Auth security upgrade
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS hashed_password VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_confirmed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_confirm_token VARCHAR",
        # Mark existing Google-linked accounts as confirmed
        "UPDATE businesses SET email_confirmed = TRUE WHERE google_id IS NOT NULL AND google_id != ''",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "ALTER TABLE card_config ADD COLUMN IF NOT EXISTS business_id UUID REFERENCES businesses(id)",
        "DELETE FROM customers WHERE email = 'placeholder_email' OR first_name = 'PLACEHOLDER_FNAME'",
        # Clean test customers: delete in FK order (push_subscriptions → referrals → stamp_transactions → loyalty_cards → customers)
        "DELETE FROM push_subscriptions WHERE card_id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM referrals WHERE referrer_card IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid) OR referred_card IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM stamp_transactions WHERE card_id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM loyalty_cards WHERE id IN ('b67ba1b0-f365-4547-8784-da4a2925ab6d'::uuid, '5b8461c4-b2ee-4b9c-a0bb-90e34fbd855f'::uuid, '76787185-18d7-4189-9bc1-a256f6f0ea6d'::uuid)",
        "DELETE FROM customers WHERE id NOT IN (SELECT DISTINCT customer_id FROM loyalty_cards WHERE customer_id IS NOT NULL)",
        # Upgrade Café Luna demo account to pro
        "UPDATE businesses SET plan='pro' WHERE slug='cafeluna'",
        "UPDATE businesses SET plan='pro' WHERE slug='sukiecookie'",
        # Upgrade suculentsc — pagó trial el 2026-03-28
        "UPDATE businesses SET plan='pro', stripe_customer_id='cus_UDxsFla6trS5Pi', stripe_subscription_id='sub_1TG1Ue0H5uUch7uMgmjd8Ygj', stripe_subscription_status='active' WHERE slug='suculentsc'",
        # ── Tiendas / Locales ──────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS stores (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, pin VARCHAR NOT NULL DEFAULT '', notes TEXT DEFAULT '', active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW())",
        "ALTER TABLE stores ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION",
        "ALTER TABLE stores ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION",
        "ALTER TABLE stores ADD COLUMN IF NOT EXISTS geo_radius_m INTEGER DEFAULT 300",
        "ALTER TABLE stores ADD COLUMN IF NOT EXISTS geo_push_msg VARCHAR DEFAULT ''",
        "ALTER TABLE stores ADD COLUMN IF NOT EXISTS address VARCHAR DEFAULT ''",
        # ── PassCodes ─────────────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS passcodes (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), code VARCHAR(16) UNIQUE NOT NULL, stamps INTEGER DEFAULT 1, used BOOLEAN DEFAULT FALSE, used_by UUID REFERENCES loyalty_cards(id), used_at TIMESTAMPTZ, expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Campañas ──────────────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS campaigns (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, subject VARCHAR DEFAULT '', body TEXT DEFAULT '', type VARCHAR DEFAULT 'email', status VARCHAR DEFAULT 'draft', segment VARCHAR DEFAULT 'all', created_at TIMESTAMPTZ DEFAULT NOW(), sent_at TIMESTAMPTZ)",
        # ── Custom QRs de Alta ────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS custom_qrs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), canal VARCHAR NOT NULL, local_name VARCHAR DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Card Programs (multi-tarjeta) ────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS card_programs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), business_id UUID REFERENCES businesses(id), name VARCHAR NOT NULL, emoji VARCHAR DEFAULT '🃏', stamps_per_reward INTEGER DEFAULT 10, reward_name VARCHAR DEFAULT 'Premio', bg_color VARCHAR DEFAULT '#0a0a0a', accent_color VARCHAR DEFAULT '#00e676', text_color VARCHAR DEFAULT '#ffffff', status VARCHAR DEFAULT 'active', sort_order INTEGER DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Birthday vouchers ─────────────────────────────────────────────────────
        "CREATE TABLE IF NOT EXISTS birthday_vouchers (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), customer_id UUID REFERENCES customers(id), business_id UUID REFERENCES businesses(id), token VARCHAR UNIQUE NOT NULL, discount_pct INTEGER DEFAULT 20, used BOOLEAN DEFAULT FALSE, used_at TIMESTAMPTZ, expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())",
        # ── Fix card_config.id to auto-increment (multi-tenant fix) ──────────────
        "CREATE SEQUENCE IF NOT EXISTS card_config_id_seq START WITH 100",
        "ALTER TABLE card_config ALTER COLUMN id SET DEFAULT nextval('card_config_id_seq')",
        "ALTER TABLE card_config ADD COLUMN IF NOT EXISTS id_fixed BOOLEAN DEFAULT FALSE",
        # ── Unique index on card_config.business_id for safe upserts ─────────────
        "CREATE UNIQUE INDEX IF NOT EXISTS uidx_card_config_business ON card_config(business_id) WHERE business_id IS NOT NULL",
        # ── Per-business email branding & custom SMTP ─────────────────────────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_from_name VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_reply_to VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_smtp_host VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_smtp_port INTEGER",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_smtp_user VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS email_smtp_pass VARCHAR",
        # ── card_programs: missing columns ────────────────────────────────────────
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS welcome_email_subject VARCHAR",
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS welcome_email_body TEXT",
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS expiry_days INTEGER",
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS max_stamps_per_visit INTEGER",
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE card_programs ADD COLUMN IF NOT EXISTS strip_bg_url TEXT",
        # Campaigns: scheduled sending support
        "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ",
        # ── Scanner device authorization ──────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS scanner_devices (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
            store_id UUID REFERENCES stores(id) ON DELETE SET NULL,
            device_token VARCHAR UNIQUE NOT NULL,
            device_name VARCHAR DEFAULT '',
            store_name VARCHAR DEFAULT '',
            status VARCHAR DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            approved_at TIMESTAMPTZ,
            last_seen_at TIMESTAMPTZ
        )""",
        # ── Audit trail: source + store_id in stamp_transactions ─────────────────
        "ALTER TABLE stamp_transactions ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT 'admin'",
        "ALTER TABLE stamp_transactions ADD COLUMN IF NOT EXISTS store_id UUID",
        # ── Stripe billing columns ────────────────────────────────────────────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS stripe_subscription_id VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS stripe_subscription_status VARCHAR",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS stripe_current_period_end TIMESTAMPTZ",
        # ── Campaign promo message (shown on Wallet pass via changeMessage) ─────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS promo_message VARCHAR DEFAULT ''",
        # ── Google Reviews ─────────────────────────────────────────────────────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS google_review_url VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS review_trigger_stamps INTEGER DEFAULT 0",
        # ── Birthday gift config ───────────────────────────────────────────────────
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_gift_type VARCHAR DEFAULT 'discount'",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_gift_product VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_email_intro VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_email_header_color VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_email_accent_color VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_email_banner_url VARCHAR DEFAULT ''",
        "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS birthday_email_footer_text VARCHAR DEFAULT ''",
        # ── Apple Wallet live update web service ──────────────────────────────────
        "ALTER TABLE loyalty_cards ADD COLUMN IF NOT EXISTS wallet_auth_token VARCHAR",
        """CREATE TABLE IF NOT EXISTS wallet_devices (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            device_library_id VARCHAR NOT NULL,
            push_token VARCHAR NOT NULL,
            card_id UUID REFERENCES loyalty_cards(id) ON DELETE CASCADE,
            pass_type_id VARCHAR NOT NULL,
            serial_number VARCHAR NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(device_library_id, card_id)
        )""",
        # Clean up orphaned wallet_devices entries left from deleted cards
        "DELETE FROM wallet_devices WHERE card_id IS NULL",

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


# ── SCHEDULED CAMPAIGNS RUNNER ────────────────────────────────────────────────
def _run_scheduled_campaigns():
    """Called by APScheduler every minute: send any campaign where scheduled_at <= NOW()."""
    from database import SessionLocal as _SL
    _db = _SL()
    try:
        rows = _db.execute(text(
            "SELECT c.id, c.business_id, c.name, c.subject, c.body, c.segment, c.type, "
            "       b.slug, b.email "
            "FROM campaigns c JOIN businesses b ON b.id=c.business_id "
            "WHERE c.status='draft' AND c.scheduled_at IS NOT NULL AND c.scheduled_at <= NOW()"
        )).fetchall()
        for row in rows:
            camp_id, bid, camp_name, subject, body_txt, segment, camp_type, slug, biz_email = row
            segment = segment or "all"
            camp_type = camp_type or "email"
            try:
                if camp_type == "push":
                    import asyncio as _asyncio
                    result = _asyncio.run(_send_apns_campaign(
                        _db,
                        business_id=str(bid),
                        title=subject or camp_name or "ZubCard",
                        message=body_txt or "",
                        segment=segment,
                    ))
                    sent = result["sent"]
                    print(f"✅ Scheduled push campaign {camp_id} sent to {sent}/{result['total_wallet_devices']} wallet devices")
                else:
                    q_base = (
                        "SELECT cu.email, cu.first_name FROM customers cu "
                        "JOIN loyalty_cards lc ON lc.customer_id=cu.id "
                        "WHERE cu.business_id=:bid AND cu.opt_in_email=TRUE "
                        "AND cu.email NOT LIKE '%placeholder%'"
                    )
                    if segment == "active":
                        q_base += " AND lc.stamps > 0"
                    customers = _db.execute(text(q_base), {"bid": str(bid)}).fetchall()
                    sent = 0
                    for cust in customers:
                        try:
                            body_html = f"<p>{(body_txt or '').replace('{nombre}', cust[1] or '')}</p>"
                            sub_text = (subject or "").replace("{nombre}", cust[1] or "")
                            if send_email(cust[0], sub_text, body_html):
                                sent += 1
                        except Exception:
                            pass
                    print(f"✅ Scheduled email campaign {camp_id} sent to {sent} customers")
                _db.execute(text(
                    "UPDATE campaigns SET status='sent', sent_at=NOW() WHERE id=:id"
                ), {"id": str(camp_id)})
                _db.commit()
            except Exception as e:
                print(f"❌ Error sending scheduled campaign {camp_id}: {e}")
                _db.rollback()
    finally:
        _db.close()


@app.on_event("startup")
def start_campaign_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(_run_scheduled_campaigns, "interval", minutes=1,
                           id="campaign_runner", replace_existing=True)
        _scheduler.start()
        print("✅ Campaign scheduler started")
    except Exception as e:
        print(f"⚠️ Campaign scheduler not started: {e}")


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
    """Verify PIN against global ADMIN_PIN, any business admin_pin, or any active store PIN from DB"""
    if str(pin) == str(ADMIN_PIN):
        return
    # If db provided, also check any business's admin_pin OR any active store PIN
    if db:
        biz = db.query(models.Business).filter(models.Business.admin_pin == str(pin)).first()
        if biz:
            return
        # Accept store employee PINs (enables scanner stamp adding with store PIN)
        try:
            store_row = db.execute(
                text("SELECT id FROM stores WHERE pin=:pin AND active=TRUE LIMIT 1"),
                {"pin": str(pin)},
            ).fetchone()
            if store_row:
                return
        except Exception:
            pass
    raise HTTPException(status_code=403, detail="PIN incorrecto")


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



def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    from_name: str = "",
    reply_to: str = "",
    smtp_host: str = "",
    smtp_port: int = 0,
    smtp_user: str = "",
    smtp_pass: str = "",
) -> bool:
    """
    Send email. Returns True if sent, False otherwise.

    When SMTP_USER == "resend", uses Resend's HTTP API (avoids SMTP port blocking).
    Otherwise falls back to SMTP with starttls.
    Per-business overrides take precedence over global SMTP_* env vars.
    """
    import urllib.request as _urlreq
    _host  = smtp_host or SMTP_HOST
    _port  = smtp_port or SMTP_PORT
    _user  = smtp_user or SMTP_USER
    _pass  = smtp_pass or SMTP_PASS
    _from_addr = SMTP_FROM or _user   # actual sender address
    _from_name = from_name.strip() if from_name else ""

    if not _user or not _pass:
        print(f"Email NOT sent (not configured): to={to_email}, subject={subject}")
        return False

    # ── Resend HTTP API via httpx (bypasses Cloudflare TLS fingerprint block) ────
    if _user.lower() == "resend":
        try:
            from email.utils import formataddr as _fmtaddr
            _from_field = _fmtaddr((_from_name, _from_addr)) if _from_name else _from_addr
            payload = {
                "from": _from_field,
                "to": [to_email],
                "subject": subject,
                "html": html_body,
            }
            if reply_to:
                payload["reply_to"] = reply_to
            with httpx.Client(timeout=15) as client:
                response = client.post(
                    "https://api.resend.com/emails",
                    json=payload,
                    headers={"Authorization": f"Bearer {_pass}"},
                )
            if response.status_code in (200, 201):
                print(f"✅ Email sent via Resend API (httpx): to={to_email}, from={_from_field}")
                return True
            else:
                print(f"❌ Resend API error {response.status_code}: {response.text[:200]}")
                return False
        except Exception as e:
            print(f"❌ Resend API (httpx) error: {e}")
            return False

    # ── Standard SMTP (used for Resend SMTP relay and other providers) ──────────
    if not _host:
        print(f"Email NOT sent (SMTP_HOST not set): to={to_email}")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        if _from_name:
            from email.utils import formataddr
            msg["From"] = formataddr((_from_name, _from_addr))
        else:
            msg["From"] = _from_addr
        msg["To"] = to_email
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        with smtplib.SMTP(_host, _port, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.login(_user, _pass)
            server.sendmail(_from_addr, [to_email], msg.as_string())
        print(f"✅ Email sent via SMTP: to={to_email}, from_name={_from_name or _from_addr}")
        return True
    except Exception as e:
        print(f"❌ SMTP error: {e}")
        return False


def render_welcome_email(
    name: str,
    card_url: str,
    stamps: int = 0,
    referral_code: str = "",
    referral_url: str = "",
    wallet_url: str = "",
    google_wallet_url: str = "",
    # Card program branding (so email matches the actual card design)
    card_bg_color: str = "#26170c",
    card_accent_color: str = "#ffca48",
    card_text_color: str = "#ffffff",
    card_emoji: str = "⭐",
    card_reward_name: str = "Premio",
    card_stamps_per_reward: int = 10,
    card_name: str = "",
    card_biz_name: str = "ZubCard",
    card_logo_url: str = "",
) -> str:
    """Render welcome email HTML with card branding"""
    template = templates.get_template("email_welcome.html")
    return template.render(
        name=name,
        card_url=card_url,
        stamps=stamps,
        referral_code=referral_code,
        referral_url=referral_url,
        wallet_url=wallet_url,
        google_wallet_url=google_wallet_url,
        subject="¡Bienvenido/a! 🎉",
        card_bg_color=card_bg_color,
        card_accent_color=card_accent_color,
        card_text_color=card_text_color,
        card_emoji=card_emoji,
        card_reward_name=card_reward_name,
        card_stamps_per_reward=card_stamps_per_reward,
        card_name=card_name,
        card_biz_name=card_biz_name,
        card_logo_url=card_logo_url,
    )


def _prog_email_kwargs(prog, biz=None) -> dict:
    """Extract card program branding kwargs for render_welcome_email."""
    if not prog:
        return {}
    return dict(
        card_bg_color=prog.bg_color or "#26170c",
        card_accent_color=prog.accent_color or "#ffca48",
        card_text_color=prog.text_color or "#ffffff",
        card_emoji=prog.emoji or "⭐",
        card_reward_name=prog.reward_name or "Premio",
        card_stamps_per_reward=prog.stamps_per_reward or 10,
        card_name=prog.name or "",
        card_biz_name=(biz.name if biz else "") or (prog.name or ""),
        card_logo_url=(biz.logo_url if biz else "") or "",
    )


def render_birthday_email(name: str, card_url: str) -> str:
    template = templates.get_template("email_birthday.html")
    return template.render(name=name, card_url=card_url)


def card_to_dict(card: models.LoyaltyCard, customer: models.Customer, stamps_per_reward: int = None) -> dict:
    full_name = f"{customer.first_name or ''} {customer.last_name or ''}".strip()
    _spr = stamps_per_reward if stamps_per_reward else STAMPS_PER_REWARD
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
        "stampsOnCard":   _spr,
        "totalStamps":    card.total_stamps or 0,
        "awardBalance":   card.award_balance or 0,
        "rewardsRedeemed": card.rewards_redeemed or 0,
        "awardTotal":     (card.rewards_redeemed or 0) + (card.award_balance or 0),
        "tier":           None,  # VIP tiers removed
        "createdAt":      customer.created_at.isoformat() if customer.created_at else "",
        "updatedAt":      card.updated_at.isoformat() if card.updated_at else "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/health")
def health():
    return {"status": "ok", "service": "ZubCard"}


@app.get("/api/smtp-status")
def smtp_status():
    """Non-sensitive SMTP config check — shows whether vars are set (not their values)."""
    return {
        "SMTP_HOST_set":  bool(SMTP_HOST),
        "SMTP_USER_set":  bool(SMTP_USER),
        "SMTP_PASS_set":  bool(SMTP_PASS),
        "SMTP_FROM":      SMTP_FROM,
        "SMTP_HOST_hint": SMTP_HOST[:6] + "…" if SMTP_HOST else "",
        "SMTP_USER_hint": SMTP_USER.split("@")[0][:3] + "…@" + SMTP_USER.split("@")[1] if SMTP_USER and "@" in SMTP_USER else "",
        "SMTP_USER_is_resend": SMTP_USER.lower() == "resend" if SMTP_USER else False,
    }


@app.post("/api/admin/smtp-test")
async def smtp_test(request: Request, db: Session = Depends(get_db)):
    """Send a real test email and return detailed result (admin only)."""
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    to_email = body.get("to", "")
    if not to_email:
        raise HTTPException(status_code=400, detail="Falta 'to' email")

    import urllib.request as _urlreq
    _user = SMTP_USER
    _pass = SMTP_PASS
    _host = SMTP_HOST
    _from = SMTP_FROM or _user
    error_detail = ""
    sent = False

    method_used = "unknown"
    try:
        # Use httpx for Resend API — bypasses Cloudflare TLS fingerprint block on urllib
        if (_user or "").lower() == "resend":
            method_used = "resend_api_httpx"
            payload = {"from": _from, "to": [to_email], "subject": "Test email - SukieCard", "html": "<p>Test OK ✅</p>"}
            with httpx.Client(timeout=15) as client:
                response = client.post(
                    "https://api.resend.com/emails",
                    json=payload,
                    headers={"Authorization": f"Bearer {_pass}"},
                )
            if response.status_code in (200, 201):
                sent = True
                error_detail = f"Resend API OK: {response.text[:120]}"
            else:
                error_detail = f"Resend API {response.status_code}: {response.text[:400]}"
        else:
            method_used = "smtp"
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText("<p>Test OK ✅</p>", "html", "utf-8")
            msg["Subject"] = "Test email - SukieCard"
            msg["From"] = _from
            msg["To"] = to_email
            with smtplib.SMTP(_host, SMTP_PORT, timeout=10) as server:
                server.ehlo(); server.starttls(); server.login(_user, _pass)
                server.sendmail(_from, [to_email], msg.as_string())
            sent = True
            error_detail = f"SMTP OK via {_host}:{SMTP_PORT}"
    except Exception as e:
        error_detail = str(e)

    return {
        "sent": sent,
        "to": to_email,
        "from": _from,
        "method": method_used,
        "error": error_detail if not sent else None,
        "detail": error_detail,
    }


@app.get("/api/admin/resend-dns")
def resend_dns(pin: str, domain_id: str = "", db: Session = Depends(get_db)):
    """Fetch Resend domain details and trigger verification."""
    verify_pin(pin, db)
    import urllib.request as _urlreq, json as _json
    if not SMTP_PASS:
        return {"error": "SMTP_PASS not set"}
    try:
        results = {}
        # Try to get domain details if domain_id provided
        if domain_id:
            try:
                req = _urlreq.Request(f"https://api.resend.com/domains/{domain_id}",
                    headers={"Authorization": f"Bearer {SMTP_PASS}"})
                with _urlreq.urlopen(req, timeout=15) as r:
                    results["domain"] = _json.loads(r.read().decode())
            except Exception as e:
                results["domain_error"] = str(e)

            # Trigger verification
            try:
                req2 = _urlreq.Request(
                    f"https://api.resend.com/domains/{domain_id}/verify",
                    data=b"{}",
                    headers={"Authorization": f"Bearer {SMTP_PASS}", "Content-Type": "application/json"},
                    method="POST"
                )
                with _urlreq.urlopen(req2, timeout=15) as r2:
                    results["verify"] = _json.loads(r2.read().decode())
            except Exception as e:
                results["verify_error"] = str(e)

            return results

        # List all domains
        req3 = _urlreq.Request("https://api.resend.com/domains",
            headers={"Authorization": f"Bearer {SMTP_PASS}"})
        with _urlreq.urlopen(req3, timeout=15) as r3:
            return _json.loads(r3.read().decode())
    except Exception as e:
        return {"error": str(e)}


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
# APPLE WALLET WEB SERVICE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _wallet_get_or_create_token(card, db: Session) -> str:
    """Return existing wallet_auth_token or generate a new one and persist it."""
    token = getattr(card, "wallet_auth_token", None)
    if not token:
        token = secrets.token_urlsafe(32)
        db.execute(
            text("UPDATE loyalty_cards SET wallet_auth_token=:tok WHERE id=:cid"),
            {"tok": token, "cid": str(card.id)},
        )
        db.commit()
        db.refresh(card)
    return token


def _wallet_serial_to_card_id(serial: str, db: Session):
    """Convert wallet serial (card_id without dashes, first 20 chars) → card row."""
    row = db.execute(
        text("SELECT id FROM loyalty_cards WHERE REPLACE(id::text, '-', '') LIKE :prefix"),
        {"prefix": serial + "%"},
    ).fetchone()
    return row[0] if row else None


def _wallet_verify_auth(serial: str, auth_header: str, db: Session):
    """Verify Apple Wallet Authorization header matches the card's auth token."""
    token = (auth_header or "").replace("ApplePass ", "").strip()
    card_id = _wallet_serial_to_card_id(serial, db)
    if not card_id:
        raise HTTPException(status_code=401, detail="Pass not found")
    row = db.execute(
        text("SELECT wallet_auth_token FROM loyalty_cards WHERE id=:cid"),
        {"cid": str(card_id)},
    ).fetchone()
    stored = row[0] if row else None
    if not stored or stored != token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return card_id


async def _apns_send(push_token: str, payload: dict, push_type: str = "background", priority: int = 5) -> bool:
    """Core APNs HTTP/2 sender. Used for both wallet updates and campaign alerts."""
    import base64, ssl, tempfile, os as _os
    p12_b64   = _os.environ.get("APPLE_P12_B64", "")
    p12_pass  = _os.environ.get("APPLE_P12_PASSWORD", "").encode()
    pass_type = _os.environ.get("APPLE_PASS_TYPE_ID", "")
    if not (p12_b64 and pass_type):
        print("⚠️  APPLE_P12_B64 or APPLE_PASS_TYPE_ID not set — skipping APNs send")
        return False
    try:
        from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
        p12_bytes = base64.b64decode(p12_b64)
        priv_key, cert, _ = load_key_and_certificates(p12_bytes, p12_pass)
        pem_key  = priv_key.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption())
        pem_cert = cert.public_bytes(Encoding.PEM)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as kf:
            kf.write(pem_key); key_path = kf.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as cf:
            cf.write(pem_cert); cert_path = cf.name
        try:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
            ssl_ctx.check_hostname = True
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
            ssl_ctx.load_default_certs()
            apn_url = f"https://api.push.apple.com/3/device/{push_token}"
            headers = {
                "apns-topic": pass_type,
                "apns-push-type": push_type,
                "apns-priority": str(priority),
            }
            async with httpx.AsyncClient(http2=True, verify=ssl_ctx) as client:
                resp = await client.post(apn_url, json=payload, headers=headers)
                ok = resp.status_code == 200
                print(f"APNs {push_type} → {resp.status_code} for token …{push_token[-8:]}")
                if not ok:
                    print(f"APNs error body: {resp.text[:200]}")
                return ok
        finally:
            _os.unlink(key_path)
            _os.unlink(cert_path)
    except Exception as ex:
        print(f"APNs send error: {ex}")
        return False


async def _push_apple_wallet(push_token: str) -> bool:
    """Send silent background push to tell Wallet app to refresh the pass."""
    return await _apns_send(push_token, payload={}, push_type="background", priority=5)


async def _push_apple_alert(push_token: str, title: str, body: str) -> bool:
    """Send a visible push notification alert via Apple Wallet APNs."""
    payload = {"aps": {"alert": {"title": title, "body": body}, "sound": "default"}}
    return await _apns_send(push_token, payload=payload, push_type="alert", priority=10)


async def _push_wallet_update(card_id, db: Session):
    """Find all registered devices for this card and fire APNs pushes."""
    try:
        rows = db.execute(
            text("SELECT push_token FROM wallet_devices WHERE card_id=:cid"),
            {"cid": str(card_id)},
        ).fetchall()
        if not rows:
            print(f"No wallet devices registered for card {card_id}")
            return
        print(f"Pushing APNs update to {len(rows)} device(s) for card {card_id}")
        for row in rows:
            push_token = row[0]
            try:
                result = await _push_apple_wallet(push_token)
                print(f"APNs push result for token ...{push_token[-8:]}: {result}")
            except Exception as ex:
                print(f"Push dispatch error: {ex}")
    except Exception as ex:
        print(f"_push_wallet_update error: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# APPLE WALLET WEB SERVICE ENDPOINTS
# https://developer.apple.com/library/archive/documentation/PassKit/Reference/PassKit_WebService/WebService.html
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/wallet/v1/devices/{device_library_id}/registrations/{pass_type_id}/{serial_number}")
async def wallet_register_device(
    device_library_id: str,
    pass_type_id: str,
    serial_number: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Apple calls this when a pass is added to Wallet — register device/push token."""
    auth_header = request.headers.get("Authorization", "")
    print(f"📲 Wallet registration: serial={serial_number} device={device_library_id[:12]}... passType={pass_type_id}")
    try:
        card_id = _wallet_verify_auth(serial_number, auth_header, db)
        print(f"  ✅ Auth OK → card_id={card_id}")
    except Exception as auth_err:
        print(f"  ❌ Auth FAILED for serial={serial_number}: {auth_err}")
        raise

    body = await request.json()
    push_token = body.get("pushToken", "")
    print(f"  push_token={push_token[:12]}... (len={len(push_token)})")
    if not push_token:
        raise HTTPException(status_code=400, detail="pushToken required")

    # Upsert: insert or update push_token on conflict
    existing = db.execute(
        text("SELECT id FROM wallet_devices WHERE device_library_id=:did AND card_id=:cid"),
        {"did": device_library_id, "cid": str(card_id)},
    ).fetchone()

    if existing:
        db.execute(
            text("UPDATE wallet_devices SET push_token=:pt WHERE device_library_id=:did AND card_id=:cid"),
            {"pt": push_token, "did": device_library_id, "cid": str(card_id)},
        )
        db.commit()
        return JSONResponse(status_code=200, content={"status": "updated"})
    else:
        db.execute(
            text("""INSERT INTO wallet_devices
                    (id, device_library_id, push_token, card_id, pass_type_id, serial_number)
                    VALUES (gen_random_uuid(), :did, :pt, :cid, :ptid, :sn)"""),
            {
                "did": device_library_id,
                "pt": push_token,
                "cid": str(card_id),
                "ptid": pass_type_id,
                "sn": serial_number,
            },
        )
        db.commit()
        return JSONResponse(status_code=201, content={"status": "registered"})


@app.delete("/api/wallet/v1/devices/{device_library_id}/registrations/{pass_type_id}/{serial_number}")
async def wallet_unregister_device(
    device_library_id: str,
    pass_type_id: str,
    serial_number: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Apple calls this when a pass is removed from Wallet."""
    auth_header = request.headers.get("Authorization", "")
    card_id = _wallet_verify_auth(serial_number, auth_header, db)
    db.execute(
        text("DELETE FROM wallet_devices WHERE device_library_id=:did AND card_id=:cid"),
        {"did": device_library_id, "cid": str(card_id)},
    )
    db.commit()
    return JSONResponse(status_code=200, content={"status": "unregistered"})


@app.get("/api/wallet/v1/devices/{device_library_id}/registrations/{pass_type_id}")
def wallet_list_updatable_passes(
    device_library_id: str,
    pass_type_id: str,
    passesUpdatedSince: str = None,
    db: Session = Depends(get_db),
):
    """Apple polls this to find passes that have been updated since last check."""
    query = """
        SELECT wd.serial_number, lc.updated_at
        FROM wallet_devices wd
        JOIN loyalty_cards lc ON lc.id = wd.card_id
        WHERE wd.device_library_id = :did
          AND wd.pass_type_id = :ptid
    """
    params = {"did": device_library_id, "ptid": pass_type_id}
    if passesUpdatedSince:
        # URL query params decode + as space; fix timezone offset before passing to DB
        passesUpdatedSince = passesUpdatedSince.replace(" ", "+")
        query += " AND lc.updated_at > :since"
        passesUpdatedSince = passesUpdatedSince.replace(" ", "+")
        params["since"] = passesUpdatedSince

    rows = db.execute(text(query), params).fetchall()
    if not rows:
        return JSONResponse(status_code=204, content=None)

    serials = [r[0] for r in rows]
    last_updated = max(r[1] for r in rows if r[1])
    return {
        "serialNumbers": serials,
        "lastUpdated": last_updated.isoformat() if last_updated else datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/wallet/v1/passes/{pass_type_id}/{serial_number}")
def wallet_get_updated_pass(
    pass_type_id: str,
    serial_number: str,
    request: Request,
    db: Session = Depends(get_db),
):
    """Apple calls this to get the latest version of a pass."""
    from fastapi.responses import Response as FResponse
    auth_header = request.headers.get("Authorization", "")
    card_id = _wallet_verify_auth(serial_number, auth_header, db)

    card = db.query(models.LoyaltyCard).filter(models.LoyaltyCard.id == card_id).first()
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    # Gather business/program settings (same logic as wallet download)
    stamps_per_reward = STAMPS_PER_REWARD
    reward_name = "Premio"
    biz_name = "Zubie Card"
    primary_color = "#26170c"
    accent_color = "#ffca48"
    text_color_val = "#ffffff"
    strip_bg_url_val = ""
    biz_logo_url = ""
    biz_lat = biz_lng = biz_geo_msg = None
    biz_geo_radius = 300

    if customer and customer.business_id:
        biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if biz:
            biz_name = biz.name or biz_name
            stamps_per_reward = biz.stamps_per_reward or stamps_per_reward
            primary_color = biz.primary_color or primary_color
            accent_color = biz.accent_color or accent_color
            biz_logo_url = biz.logo_url or ""
            biz_lat = getattr(biz, "latitude", None)
            biz_lng = getattr(biz, "longitude", None)
            biz_geo_msg = getattr(biz, "geo_push_msg", None) or ""
            biz_geo_radius = getattr(biz, "geo_radius_m", 300) or 300
        prog = db.query(models.CardProgram).filter(
            models.CardProgram.business_id == customer.business_id
        ).first()
        if prog:
            reward_name = prog.reward_name or reward_name
            strip_bg_url_val = getattr(prog, "strip_bg_url", None) or ""
            text_color_val = getattr(prog, "text_color", None) or "#ffffff"
            primary_color = prog.bg_color or primary_color
            accent_color = prog.accent_color or accent_color

    auth_token = getattr(card, "wallet_auth_token", "") or ""
    # Promo message from business (shown as Wallet changeMessage notification)
    biz_promo_msg = ""
    extra_locs = []
    if customer and customer.business_id:
        _pm_biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if _pm_biz:
            biz_promo_msg = getattr(_pm_biz, "promo_message", None) or ""
        # Collect geo from all active stores
        _store_rows = db.execute(text(
            "SELECT latitude, longitude, geo_radius_m, geo_push_msg FROM stores "
            "WHERE business_id=:bid AND active=TRUE AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ), {"bid": str(customer.business_id)}).fetchall()
        extra_locs = [{"lat": r[0], "lng": r[1], "radius": r[2] or 300, "msg": r[3] or ""} for r in _store_rows]

    try:
        from wallet_pass import generate_pkpass
        pkpass_bytes = generate_pkpass(
            card_id=str(card.id),
            first_name=customer.first_name if customer else "Cliente",
            last_name=customer.last_name if customer else "",
            stamps=card.stamps or 0,
            stamps_per_reward=stamps_per_reward,
            reward_name=reward_name,
            biz_name=biz_name,
            primary_color=primary_color,
            accent_color=accent_color,
            text_color=text_color_val,
            latitude=biz_lat,
            longitude=biz_lng,
            geo_push_msg=biz_geo_msg or "",
            geo_radius_m=biz_geo_radius,
            strip_bg_url=strip_bg_url_val,
            logo_url=biz_logo_url,
            auth_token=auth_token,
            award_balance=card.award_balance or 0,
            promo_message=biz_promo_msg,
            extra_locations=extra_locs,
        )
        return FResponse(
            content=pkpass_bytes,
            media_type="application/vnd.apple.pkpass",
            headers={
                "Last-Modified": datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"),
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando pass: {str(e)}")


@app.post("/api/wallet/v1/log")
async def wallet_log(request: Request):
    """Apple sends diagnostic logs here — just acknowledge."""
    try:
        body = await request.json()
        print(f"Apple Wallet log: {body}")
    except Exception:
        pass
    return JSONResponse(status_code=200, content={})


# ══════════════════════════════════════════════════════════════════════════════
# TARJETA PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/card/{card_id}/wallet.pkpass")
def download_wallet_pass(card_id: str, db: Session = Depends(get_db)):
    """Generate and return an Apple Wallet .pkpass for this loyalty card."""
    from fastapi.responses import Response as FResponse
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    # Get business-specific card program settings
    stamps_per_reward = STAMPS_PER_REWARD
    reward_name = "Premio"
    biz_name = "Zubie Card"
    primary_color = "#26170c"
    accent_color = "#ffca48"
    biz_logo_url = ""
    if customer and customer.business_id:
        biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if biz:
            biz_name = biz.name or biz_name
            stamps_per_reward = biz.stamps_per_reward or stamps_per_reward
            primary_color = biz.primary_color or primary_color
            accent_color = biz.accent_color or accent_color
            biz_logo_url = biz.logo_url or ""
        prog = db.query(models.CardProgram).filter(
            models.CardProgram.business_id == customer.business_id
        ).first()
        if prog:
            reward_name = prog.reward_name or reward_name

    # Pull strip background, text color + geo config from the business/program record
    strip_bg_url_val = ""
    text_color_val   = "#ffffff"
    biz_lat = biz_lng = biz_geo_msg = None
    biz_geo_radius = 300
    if customer and customer.business_id:
        _geo_biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if _geo_biz:
            biz_lat        = getattr(_geo_biz, "latitude", None)
            biz_lng        = getattr(_geo_biz, "longitude", None)
            biz_geo_msg    = getattr(_geo_biz, "geo_push_msg", None) or ""
            biz_geo_radius = getattr(_geo_biz, "geo_radius_m", 300) or 300
        _prog2 = db.query(models.CardProgram).filter(
            models.CardProgram.business_id == customer.business_id
        ).first()
        if _prog2:
            strip_bg_url_val = getattr(_prog2, "strip_bg_url", None) or ""
            text_color_val   = getattr(_prog2, "text_color", None) or "#ffffff"
            # Card creator stores colors in CardProgram — always override Business defaults
            primary_color    = _prog2.bg_color or primary_color
            accent_color     = _prog2.accent_color or accent_color

    # Generate or reuse per-card wallet auth token for live update web service
    auth_token = _wallet_get_or_create_token(card, db)
    # Promo message + extra store locations
    _dl_promo = ""
    _dl_extra_locs = []
    if customer and customer.business_id:
        _dl_biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if _dl_biz:
            _dl_promo = getattr(_dl_biz, "promo_message", None) or ""
        _dl_store_rows = db.execute(text(
            "SELECT latitude, longitude, geo_radius_m, geo_push_msg FROM stores "
            "WHERE business_id=:bid AND active=TRUE AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ), {"bid": str(customer.business_id)}).fetchall()
        _dl_extra_locs = [{"lat": r[0], "lng": r[1], "radius": r[2] or 300, "msg": r[3] or ""} for r in _dl_store_rows]

    try:
        from wallet_pass import generate_pkpass
        pkpass_bytes = generate_pkpass(
            card_id=str(card.id),
            first_name=customer.first_name if customer else "Cliente",
            last_name=customer.last_name if customer else "",
            stamps=card.stamps or 0,
            stamps_per_reward=stamps_per_reward,
            reward_name=reward_name,
            biz_name=biz_name,
            primary_color=primary_color,
            accent_color=accent_color,
            text_color=text_color_val,
            latitude=biz_lat,
            longitude=biz_lng,
            geo_push_msg=biz_geo_msg or "",
            geo_radius_m=biz_geo_radius,
            strip_bg_url=strip_bg_url_val,
            logo_url=biz_logo_url,
            auth_token=auth_token,
            award_balance=card.award_balance or 0,
            promo_message=_dl_promo,
            extra_locations=_dl_extra_locs,
        )
        return FResponse(
            content=pkpass_bytes,
            media_type="application/vnd.apple.pkpass",
            headers={"Content-Disposition": f'attachment; filename="tarjeta-{str(card.id)[:8]}.pkpass"'},
        )
    except ValueError as ve:
        raise HTTPException(status_code=503, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando wallet pass: {str(e)}")


@app.get("/card/{card_id}", response_class=HTMLResponse)
def show_card(card_id: str, request: Request, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    first_name = customer.first_name if customer else "Cliente"

    # Load business info for card styling and wallet
    biz = None
    if customer and customer.business_id:
        biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()

    # Load CardProgram for this business (the card creator stores colors here)
    _show_prog = db.query(models.CardProgram).filter(
        models.CardProgram.business_id == customer.business_id
    ).first() if customer and customer.business_id else None
    # CardProgram.bg_color is the source of truth; fall back to Business colors
    primary_color        = (_show_prog.bg_color      if _show_prog and _show_prog.bg_color      else None) or (biz.primary_color if biz else None) or "#26170c"
    accent_color         = (_show_prog.accent_color  if _show_prog and _show_prog.accent_color  else None) or (biz.accent_color  if biz else None) or "#ffca48"
    biz_name             = (biz.name               if biz else None) or ""
    biz_slug             = (biz.slug               if biz else None) or ""
    stamps_per_reward_val = (biz.stamps_per_reward if biz else None) or STAMPS_PER_REWARD
    google_review_url    = getattr(biz, "google_review_url", "") or "" if biz else ""
    review_trigger_stamps = getattr(biz, "review_trigger_stamps", 0) or 0 if biz else 0

    # Load business-scoped card_title for the page header
    card_title = (biz.card_title if biz else None) or biz_name or CARD_TITLE
    if customer and customer.business_id:
        row = db.execute(
            text("SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"),
            {"bid": str(customer.business_id)}
        ).fetchone()
        if row:
            cfg = json.loads(row[0]) if row[0] else {}
            card_title = cfg.get("general", {}).get("card_title") or \
                         cfg.get("general", {}).get("card_name") or card_title

    # Generate Google Wallet URL
    card_url = f"{BASE_URL}/card/{card_id}"
    _gw_reward = (_show_prog.reward_name if _show_prog and _show_prog.reward_name else None) or "Premio"
    _gw_logo   = (biz.logo_url if biz and biz.logo_url else "") or ""
    _gw_cname  = (_show_prog.name if _show_prog and _show_prog.name else None) or biz_name
    google_wallet_url = generate_google_wallet_url(
        card_id=card_id,
        biz_slug=biz_slug,
        biz_name=biz_name,
        customer_name=first_name,
        stamps=card.stamps or 0,
        stamps_per_reward=stamps_per_reward_val,
        card_url=card_url,
        primary_color=primary_color,
        accent_color=accent_color,
        reward_name=_gw_reward,
        logo_url=_gw_logo,
        award_balance=card.award_balance or 0,
        card_name=_gw_cname,
    ) if biz_slug else None

    return templates.TemplateResponse("card.html", {
        "request":           request,
        "card_id":           card_id,
        "first_name":        first_name,
        "name":              first_name,
        "card_title":        card_title,
        "api_base":          BASE_URL,
        "stamps":            card.stamps or 0,
        "stamps_per_reward": stamps_per_reward_val,
        "rewards_redeemed":  card.rewards_redeemed or 0,
        "award_balance":     card.award_balance or 0,
        "total_stamps":      card.total_stamps or 0,
        "biz_name":          biz_name,
        "biz_slug":          biz_slug,
        "primary_color":     primary_color,
        "accent_color":      accent_color,
        "google_wallet_url":      google_wallet_url or "",
        "google_review_url":      google_review_url,
        "review_trigger_stamps":  review_trigger_stamps,
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
async def public_register(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    import traceback as _tb
    # Rate limit: 5 registrations per IP per 5 minutes
    client_ip = request.client.host if request.client else "unknown"
    check_rate_limit(f"register:{client_ip}", max_requests=5, window_seconds=300)
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

    try:
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
        db.flush()  # generate card.id before using it in StampTransaction

        tx = models.StampTransaction(card_id=card.id, stamps_added=0,
                                      transaction_type="register", note="Alta Web")
        db.add(tx)
        db.commit()
        db.refresh(card)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error al registrar: {str(e)}")

    # Handle referral
    ref_code = body.get("ref", "").strip().upper()
    if ref_code:
        try:
            ref = db.query(models.Referral).filter(
                models.Referral.code == ref_code,
                models.Referral.used == False
            ).first()
            if ref and str(ref.referrer_card) != str(card.id):
                ref.used = True
                ref.referred_card = card.id
                ref.used_at = datetime.now(timezone.utc)
                referrer_card = db.query(models.LoyaltyCard).filter(
                    models.LoyaltyCard.id == ref.referrer_card).first()
                if referrer_card:
                    referrer_card.stamps       = (referrer_card.stamps or 0) + ref.bonus_stamps
                    referrer_card.total_stamps = (referrer_card.total_stamps or 0) + ref.bonus_stamps
                card.stamps       = (card.stamps or 0) + ref.bonus_stamps
                card.total_stamps = (card.total_stamps or 0) + ref.bonus_stamps
                db.commit()
        except Exception:
            pass

    # Send welcome email (non-blocking — never break registration if email fails)
    try:
        card_url = f"{BASE_URL}/card/{card.id}"
        referral_code = ""
        referral_url  = ""
        try:
            ref_obj = db.query(models.Referral).filter(
                models.Referral.referrer_card == card.id
            ).first()
            if ref_obj:
                referral_code = ref_obj.code
                referral_url  = f"{BASE_URL}/biz/{slug}/register?ref={ref_obj.code}" if slug else ""
        except Exception:
            pass

        # Use card program's custom subject/body if configured
        email_subject = "¡Bienvenido/a! 🎉"
        email_html    = None
        _prog_for_email = None  # keep reference for branding kwargs
        if biz_id:
            try:
                _prog_for_email = db.query(models.CardProgram).filter(
                    models.CardProgram.business_id == biz_id
                ).first()
                if _prog_for_email and _prog_for_email.welcome_email_subject:
                    email_subject = _prog_for_email.welcome_email_subject
                if _prog_for_email and _prog_for_email.welcome_email_body:
                    # Simple variable substitution for custom body
                    biz_name = biz.name if biz else ""
                    custom_body = _prog_for_email.welcome_email_body
                    custom_body = custom_body.replace("{nombre}", fn)
                    custom_body = custom_body.replace("{link_tarjeta}", card_url)
                    custom_body = custom_body.replace("{negocio}", biz_name)
                    email_html = f"<html><body style='font-family:sans-serif'>{custom_body}</body></html>"
            except Exception:
                pass

        # Wallet URL — always include; if Apple certs not configured it just 404s gracefully
        wallet_url = f"{BASE_URL}/card/{card.id}/wallet.pkpass"

        # Google Wallet URL for Android
        _gw_url = ""
        try:
            _gw_biz_slug = slug or (biz.slug if biz else "")
            _gw_biz_name = biz.name if biz else ""
            _gw_prog = _prog_for_email
            _gw_color   = _gw_prog.bg_color       if _gw_prog else "#26170c"
            _gw_accent  = _gw_prog.accent_color   if _gw_prog else "#ffca48"
            _gw_spr     = _gw_prog.stamps_per_reward if _gw_prog else 10
            _gw_reward  = (_gw_prog.reward_name   if _gw_prog and _gw_prog.reward_name else None) or "Premio"
            _gw_logo2   = (biz.logo_url           if biz and biz.logo_url else "") or ""
            _gw_cname2  = (_gw_prog.name          if _gw_prog and _gw_prog.name else None) or _gw_biz_name
            if _gw_biz_slug:
                _gw_url = generate_google_wallet_url(
                    card_id=str(card.id),
                    biz_slug=_gw_biz_slug,
                    biz_name=_gw_biz_name,
                    customer_name=fn,
                    stamps=card.stamps or 0,
                    stamps_per_reward=_gw_spr,
                    card_url=card_url,
                    primary_color=_gw_color,
                    accent_color=_gw_accent,
                    reward_name=_gw_reward,
                    logo_url=_gw_logo2,
                    award_balance=card.award_balance or 0,
                    card_name=_gw_cname2,
                ) or ""
        except Exception:
            _gw_url = ""

        if email_html is None:
            email_html = render_welcome_email(
                name=fn,
                card_url=card_url,
                stamps=card.stamps or 0,
                referral_code=referral_code,
                referral_url=referral_url,
                wallet_url=wallet_url if os.environ.get("APPLE_P12_B64") else "",
                google_wallet_url=_gw_url,
                **_prog_email_kwargs(_prog_for_email, biz),
            )

        # Per-business email branding
        biz_from_name = ""
        biz_reply_to  = ""
        biz_smtp_host = ""
        biz_smtp_port = 0
        biz_smtp_user = ""
        biz_smtp_pass = ""
        if biz:
            biz_from_name = biz.email_from_name or biz.name or ""
            biz_reply_to  = biz.email_reply_to  or ""
            biz_smtp_host = biz.email_smtp_host  or ""
            biz_smtp_port = biz.email_smtp_port  or 0
            biz_smtp_user = biz.email_smtp_user  or ""
            biz_smtp_pass = biz.email_smtp_pass  or ""

        background_tasks.add_task(
            send_email,
            to_email   = email,
            subject    = email_subject,
            html_body  = email_html,
            from_name  = biz_from_name,
            reply_to   = biz_reply_to,
            smtp_host  = biz_smtp_host,
            smtp_port  = biz_smtp_port,
            smtp_user  = biz_smtp_user,
            smtp_pass  = biz_smtp_pass,
        )
    except Exception as _email_err:
        print(f"Welcome email setup failed (non-fatal): {_email_err}")

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
    # Rate limit: max 20 stamp operations per card per minute (prevents double-tap spam)
    check_rate_limit(f"stamp:{card_id}", max_requests=20, window_seconds=60)
    card = get_card_or_404(card_id, db)

    # Resolve per-business stamps_per_reward
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first() if (customer and customer.business_id) else None
    stamps_per_reward = (biz.stamps_per_reward or STAMPS_PER_REWARD) if biz else STAMPS_PER_REWARD

    n = int(body.get("stamps", 1))
    if n < 0 or n > 50:
        raise HTTPException(status_code=400, detail="stamps debe estar entre 0 y 50")

    if n > 0:
        card.stamps       = (card.stamps or 0) + n
        card.total_stamps = (card.total_stamps or 0) + n

    awards_earned = 0
    while (card.stamps or 0) >= stamps_per_reward:
        card.stamps        -= stamps_per_reward
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
    # Push live update to Apple Wallet if registered
    await _push_wallet_update(card.id, db)
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
    await _push_wallet_update(card.id, db)
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
    await _push_wallet_update(card.id, db)
    return {"message": "Premio canjeado ✅",
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
    def _src_label(src, store):
        if src == "scanner" and store: return f"📱 {store}"
        if src == "scanner": return "📱 Scanner"
        if src == "passcode": return "🔑 Passcode"
        if src == "register": return "🆕 Registro"
        return "💻 Admin"

    return [
        {"id": str(t.id), "stamps_added": t.stamps_added,
         "transaction_type": t.transaction_type or "stamp",
         "note": t.note, "store": t.store or "",
         "source": getattr(t, "source", None) or "admin",
         "source_label": _src_label(getattr(t, "source", None) or "admin", t.store or ""),
         "created_at": t.created_at.isoformat() if t.created_at else ""}
        for t in txs
    ]


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
    biz_spr = biz.stamps_per_reward if (slug and biz) else None
    customers = [card_to_dict(card, cust, stamps_per_reward=biz_spr) for card, cust in rows]

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

    from sqlalchemy import text as _text
    import traceback as _tb
    try:
        cid  = str(card.id)
        uid_row = db.execute(_text("SELECT customer_id FROM loyalty_cards WHERE id = :cid"), {"cid": cid}).fetchone()
        uid = str(uid_row[0]) if uid_row and uid_row[0] else None

        # Borrar en orden FK — todo SQL puro (usar CAST para evitar conflicto :: con params)
        db.execute(_text("DELETE FROM push_subscriptions WHERE card_id = CAST(:cid AS uuid)"), {"cid": cid})
        db.execute(_text("DELETE FROM referrals WHERE referrer_card = CAST(:cid AS uuid) OR referred_card = CAST(:cid AS uuid)"), {"cid": cid})
        db.execute(_text("UPDATE passcodes SET used_by = NULL WHERE used_by = CAST(:cid AS uuid)"), {"cid": cid})
        db.execute(_text("DELETE FROM stamp_transactions WHERE card_id = CAST(:cid AS uuid)"), {"cid": cid})
        if uid:
            db.execute(_text("DELETE FROM birthday_vouchers WHERE customer_id = CAST(:uid AS uuid)"), {"uid": uid})
        db.execute(_text("DELETE FROM loyalty_cards WHERE id = CAST(:cid AS uuid)"), {"cid": cid})
        if uid:
            other = db.execute(_text("SELECT COUNT(*) FROM loyalty_cards WHERE customer_id = CAST(:uid AS uuid)"), {"uid": uid}).scalar()
            if (other or 0) == 0:
                db.execute(_text("DELETE FROM customers WHERE id = CAST(:uid AS uuid)"), {"uid": uid})
        db.commit()
        return {"message": "Cliente eliminado"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)} | {_tb.format_exc()[-300:]}")


@app.delete("/api/admin/customers-all")
async def delete_all_customers(pin: str = "", db: Session = Depends(get_db)):
    """Borra TODOS los clientes y sus tarjetas (solo para testing)"""
    verify_pin(pin, db)
    from sqlalchemy import text as _text
    # FK order: child tables first
    db.execute(_text("DELETE FROM push_subscriptions"))
    db.execute(_text("DELETE FROM referrals"))
    db.execute(_text("UPDATE passcodes SET used_by = NULL"))
    db.execute(_text("DELETE FROM birthday_vouchers"))
    db.execute(_text("DELETE FROM stamp_transactions"))
    db.execute(_text("DELETE FROM loyalty_cards"))
    db.execute(_text("DELETE FROM customers"))
    db.commit()
    return {"message": "Todos los clientes eliminados"}


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

    biz_card_ids = None
    biz_customer_ids = None
    if slug:
        biz = get_business_by_slug(slug, db)
        if biz:
            cards = (db.query(models.LoyaltyCard)
                     .join(models.Customer, models.LoyaltyCard.customer_id == models.Customer.id)
                     .filter(models.Customer.business_id == biz.id).all())
            biz_card_ids = [c.id for c in cards]
            # Also get customer IDs for new_clients count
            custs = db.query(models.Customer).filter(
                models.Customer.business_id == biz.id,
                models.Customer.created_at >= cutoff,
                models.Customer.email != "PLACEHOLDER@sukie.internal"
            ).all()
            biz_customer_ids = [(c.id, c.created_at) for c in custs]

    txs_q = (db.query(models.StampTransaction)
             .filter(models.StampTransaction.created_at >= cutoff))

    if biz_card_ids is not None:
        if biz_card_ids:
            txs_q = txs_q.filter(models.StampTransaction.card_id.in_(biz_card_ids))
        else:
            txs_q = txs_q.filter(False)  # no cards → no transactions

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

    # Count new clients by their actual created_at date (not transaction type)
    if biz_customer_ids is not None:
        for cust_id, created_at in biz_customer_ids:
            if not created_at:
                continue
            day = created_at.strftime("%Y-%m-%d")
            if day not in by_day:
                by_day[day] = {"date": day, "stamps": 0, "redeems": 0, "new_clients": 0}
            by_day[day]["new_clients"] += 1
    else:
        # Global: count from transactions with type register
        for t in txs:
            if t.transaction_type == "register" and t.created_at:
                day = t.created_at.strftime("%Y-%m-%d")
                if day in by_day:
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


def _build_birthday_email_html(
    name: str, biz_name: str, qr_url: str,
    gift_type: str = "discount", gift_product: str = "", discount_pct: int = 20,
    hdr_color: str = "#1a1a1a", acc_color: str = "#c8a96e", banner_url: str = "",
    logo_url: str = "", email_intro: str = "", footer_text: str = "",
    is_test: bool = False, slug: str = ""
) -> str:
    """Build a premium, fully branded birthday email HTML."""
    # Fallback footer
    if not footer_text:
        footer_text = f"Con cariño, el equipo de {biz_name}"

    # Header: banner image OR solid brand color
    # banner_url may be a data URI (stored in DB) or a slug hint — always serve via /birthday-banner.jpg
    # We detect slug from banner_url pattern and build a real HTTP URL
    effective_banner = ""
    if banner_url:
        if banner_url.startswith("data:"):
            # Extract slug from context — caller must pass slug separately; use __slug hint
            # We'll use a sentinel: caller sets banner_url to the real serving URL before calling
            effective_banner = banner_url  # already resolved by caller
        else:
            effective_banner = banner_url

    if effective_banner:
        header_style = f"background:url('{effective_banner}') center/cover no-repeat;position:relative"
        header_overlay = f"<div style='position:absolute;inset:0;background:{hdr_color};opacity:.55;border-radius:12px 12px 0 0'></div>"
        header_pos = "position:relative;z-index:1"
    else:
        header_style = f"background:{hdr_color}"
        header_overlay = ""
        header_pos = ""

    # Logo block — use served URL if it's a data URI
    logo_html = ""
    if logo_url:
        served_logo = f"{BASE_URL}/biz/{slug}/logo.png" if (logo_url.startswith("data:") and slug) else logo_url
        logo_html = f"""<div style="margin-bottom:16px">
          <img src="{served_logo}" alt="{biz_name}" style="height:44px;max-width:160px;object-fit:contain;filter:brightness(0) invert(1)">
        </div>"""

    # Gift block
    gift_badge_color = acc_color
    if gift_type == "product" and gift_product:
        intro_text = email_intro or f"Muestra este QR al llegar a <strong>{biz_name}</strong>. El empleado lo escaneará y recibirás tu regalo."
        gift_html = f"""
        <div style="background:#fff;border-radius:12px;padding:24px 20px;margin-bottom:20px;border:2px solid {acc_color};text-align:center">
          <div style="font-size:2.2rem;margin-bottom:6px">🎁</div>
          <div style="font-family:Georgia,serif;font-size:1.4rem;font-weight:700;color:{hdr_color};letter-spacing:-.01em">{gift_product}</div>
          <div style="display:inline-block;margin-top:8px;background:{acc_color};color:#fff;font-size:.68rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;padding:4px 14px;border-radius:100px">Regalo exclusivo</div>
          <div style="color:#999;font-size:.75rem;margin-top:10px;letter-spacing:.03em">Válido solo hoy · Un solo uso</div>
        </div>
        <p style="color:#555;font-size:.88rem;margin:0 0 24px;line-height:1.7;text-align:center">{intro_text}</p>"""
    else:
        intro_text = email_intro or f"Muestra este QR al llegar a <strong>{biz_name}</strong>. El empleado lo escaneará y aplicará tu descuento."
        gift_html = f"""
        <div style="background:#fff;border-radius:12px;padding:24px 20px;margin-bottom:20px;border:2px solid {acc_color};text-align:center">
          <div style="font-family:Georgia,serif;font-size:3.2rem;font-weight:900;color:{hdr_color};line-height:1">{discount_pct}%</div>
          <div style="display:inline-block;margin-top:6px;background:{acc_color};color:#fff;font-size:.68rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;padding:4px 14px;border-radius:100px">de descuento</div>
          <div style="color:#999;font-size:.75rem;margin-top:10px;letter-spacing:.03em">Válido solo hoy · Un solo uso</div>
        </div>
        <p style="color:#555;font-size:.88rem;margin:0 0 24px;line-height:1.7;text-align:center">{intro_text}</p>"""

    test_banner = f"""<div style="background:#f59e0b;color:#fff;font-size:.7rem;font-weight:700;text-align:center;padding:6px;letter-spacing:.05em">
      ⚠ EMAIL DE PRUEBA — QR no válido para canjear
    </div>""" if is_test else ""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><meta name="supported-color-schemes" content="light">
</head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif;-webkit-font-smoothing:antialiased">
{test_banner}
<div style="max-width:560px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,.08)">

  <!-- HEADER -->
  <div style="{header_style};padding:44px 32px 36px;text-align:center;border-radius:12px 12px 0 0;overflow:hidden">
    {header_overlay}
    <div style="{header_pos}">
      {logo_html}
      <div style="display:inline-block;background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.35);border-radius:100px;padding:5px 16px;margin-bottom:14px">
        <span style="color:rgba(255,255,255,.95);font-size:.72rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase">🎂 Cumpleaños</span>
      </div>
      <h1 style="color:#fff;font-size:1.75rem;font-weight:800;margin:0 0 6px;letter-spacing:-.02em;text-shadow:0 1px 4px rgba(0,0,0,.2)">¡Feliz Cumpleaños, {name}!</h1>
      <p style="color:rgba(255,255,255,.85);font-size:.9rem;margin:0;font-weight:400">Hoy es tu día especial — tenemos algo para ti</p>
    </div>
  </div>

  <!-- BODY -->
  <div style="padding:32px 32px 24px">
    {gift_html}

    <!-- QR -->
    <div style="text-align:center;margin-bottom:8px">
      <div style="display:inline-block;background:#fff;border:1.5px solid #e8e8e8;border-radius:14px;padding:14px;box-shadow:0 2px 12px rgba(0,0,0,.06)">
        <img src="{qr_url}" alt="QR de regalo" width="160" height="160" style="display:block;border-radius:6px">
      </div>
      <p style="color:#bbb;font-size:.72rem;margin:10px 0 0;letter-spacing:.03em">Un solo uso · Expira hoy a las 23:59</p>
    </div>
  </div>

  <!-- DIVIDER -->
  <div style="height:1px;background:linear-gradient(90deg,transparent,#e8e8e8,transparent);margin:0 32px"></div>

  <!-- FOOTER -->
  <div style="padding:20px 32px 28px;text-align:center">
    <p style="color:#aaa;font-size:.78rem;margin:0;line-height:1.6">{footer_text}</p>
  </div>

</div>
</body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
# BIRTHDAY VOUCHERS
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/biz/{slug}/birthday-voucher/send")
async def send_birthday_voucher(slug: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    """Generate and send a birthday voucher to one or all of today's birthday customers"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    body = await request.json()
    customer_id  = body.get("customer_id")   # single customer, or None = all today
    discount_pct = int(body.get("discount_pct", 20))
    force        = bool(body.get("force", False))  # skip birthday date check (test mode)
    gift_type    = getattr(biz, "birthday_gift_type", "discount") or "discount"
    gift_product = getattr(biz, "birthday_gift_product", "") or ""
    email_intro  = getattr(biz, "birthday_email_intro", "") or ""
    hdr_color    = getattr(biz, "birthday_email_header_color", "") or getattr(biz, "primary_color", "") or "#1a1a1a"
    acc_color    = getattr(biz, "birthday_email_accent_color", "") or getattr(biz, "accent_color", "") or "#c8a96e"
    banner_url   = getattr(biz, "birthday_email_banner_url", "") or ""
    footer_text  = getattr(biz, "birthday_email_footer_text", "") or f"Con cariño, el equipo de {biz.name}"
    logo_url     = getattr(biz, "logo_url", "") or ""

    today = datetime.now().strftime("%m-%d")
    expires_at = datetime.now().replace(hour=23, minute=59, second=59)

    # Get customers to send to
    if customer_id:
        customers = db.execute(text(
            "SELECT id, first_name, last_name, email, birth_date FROM customers "
            "WHERE id=:cid AND business_id=:bid"
        ), {"cid": customer_id, "bid": str(biz.id)}).fetchall()
    else:
        customers = db.execute(text(
            "SELECT id, first_name, last_name, email, birth_date FROM customers "
            "WHERE business_id=:bid AND birth_date IS NOT NULL AND birth_date != ''"
        ), {"bid": str(biz.id)}).fetchall()
        customers = [c for c in customers if len(c.birth_date or "") >= 10 and c.birth_date[5:10] == today]

    sent = 0
    errors = []
    for cust in customers:
        if not cust.email:
            continue
        # Check if voucher already sent today
        existing = db.execute(text(
            "SELECT id FROM birthday_vouchers WHERE customer_id=:cid AND business_id=:bid "
            "AND created_at >= CURRENT_DATE"
        ), {"cid": str(cust.id), "bid": str(biz.id)}).fetchone()
        if existing and not force:
            continue

        # Create voucher token
        token = str(uuid.uuid4()).replace("-", "")[:24]
        db.execute(text(
            "INSERT INTO birthday_vouchers (customer_id, business_id, token, discount_pct, expires_at) "
            "VALUES (:cid, :bid, :token, :disc, :exp)"
        ), {"cid": str(cust.id), "bid": str(biz.id), "token": token, "disc": discount_pct, "exp": expires_at})
        db.commit()

        # Build voucher URL (for scanner to scan)
        voucher_url = f"{BASE_URL}/biz/{slug}/birthday/{token}"
        # QR served as URL (not base64 — Gmail blocks data: URIs)
        qr_url = f"{BASE_URL}/biz/{slug}/birthday/{token}/qr.png"

        name = (cust.first_name or "Cliente").strip()
        subject = f"¡Feliz Cumpleaños, {name}! Tu regalo de {biz.name} te espera"

        # Use served URL for banner (Gmail blocks data: URIs)
        served_banner = f"{BASE_URL}/biz/{slug}/birthday-banner.jpg" if banner_url else ""
        html = _build_birthday_email_html(
            name=name, biz_name=biz.name, qr_url=qr_url,
            gift_type=gift_type, gift_product=gift_product, discount_pct=discount_pct,
            hdr_color=hdr_color, acc_color=acc_color, banner_url=served_banner,
            logo_url=logo_url, email_intro=email_intro, footer_text=footer_text,
            is_test=False, slug=slug
        )
        try:
            if send_email(to_email=cust.email, subject=subject, html_body=html):
                sent += 1
            else:
                errors.append(f"Email no enviado a {cust.email}")
        except Exception as e:
            errors.append(str(e))

    return {"sent": sent, "errors": errors}


@app.post("/api/biz/{slug}/birthday-voucher/redeem")
async def redeem_birthday_voucher(slug: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    """Scanner redeems a birthday voucher token"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    body = await request.json()
    token = body.get("token", "").strip()

    row = db.execute(text(
        "SELECT bv.id, bv.discount_pct, bv.used, bv.expires_at, "
        "c.first_name, c.last_name, c.email "
        "FROM birthday_vouchers bv "
        "JOIN customers c ON c.id = bv.customer_id "
        "WHERE bv.token=:token AND bv.business_id=:bid"
    ), {"token": token, "bid": str(biz.id)}).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="QR no válido")
    if row.used:
        raise HTTPException(status_code=400, detail="Este regalo ya fue canjeado")
    if row.expires_at < datetime.now():
        raise HTTPException(status_code=400, detail="Este regalo ha expirado")

    # Mark as used
    db.execute(text(
        "UPDATE birthday_vouchers SET used=TRUE, used_at=NOW() WHERE id=:id"
    ), {"id": str(row.id)})
    db.commit()

    return {
        "ok": True,
        "customer_name": f"{row.first_name or ''} {row.last_name or ''}".strip(),
        "discount_pct": row.discount_pct,
        "message": f"🎂 {(row.first_name or 'Cliente')} tiene un {row.discount_pct}% de descuento hoy por su cumpleaños"
    }


@app.get("/biz/{slug}/birthday/test-preview/qr.png")
def birthday_test_qr(slug: str):
    """Return a test QR PNG for birthday email previews."""
    import io as _io
    try:
        import qrcode as _qrcode
        img = _qrcode.make(f"{BASE_URL}/biz/{slug}/birthday/TEST")
        buf = _io.BytesIO(); img.save(buf, format="PNG"); buf.seek(0)
        return StreamingResponse(buf, media_type="image/png", headers={"Cache-Control": "public, max-age=3600"})
    except Exception:
        from PIL import Image as _PILImage
        img = _PILImage.new("RGB", (180, 180), color=(255, 255, 255)); buf = _io.BytesIO()
        img.save(buf, format="PNG"); buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")


@app.get("/biz/{slug}/birthday/{token}/qr.png")
def birthday_voucher_qr(slug: str, token: str, db: Session = Depends(get_db)):
    """Return QR code as PNG image — safe for email embedding via URL."""
    import io as _io
    try:
        import qrcode as _qrcode
        voucher_url = f"{BASE_URL}/biz/{slug}/birthday/{token}"
        qr_img = _qrcode.make(voucher_url)
        buf = _io.BytesIO()
        qr_img.save(buf, format="PNG")
        buf.seek(0)
        return StreamingResponse(buf, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
    except Exception:
        from PIL import Image as _PILImage
        img = _PILImage.new("RGB", (180, 180), color=(255, 255, 255))
        buf = _io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")


@app.get("/biz/{slug}/birthday/{token}", response_class=HTMLResponse)
def birthday_voucher_page(slug: str, token: str, db: Session = Depends(get_db)):
    """Customer-facing birthday voucher page with QR"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        return HTMLResponse("<h2>Negocio no encontrado</h2>", status_code=404)

    row = db.execute(text(
        "SELECT bv.id, bv.discount_pct, bv.used, bv.expires_at, "
        "c.first_name FROM birthday_vouchers bv "
        "JOIN customers c ON c.id = bv.customer_id "
        "WHERE bv.token=:token AND bv.business_id=:bid"
    ), {"token": token, "bid": str(biz.id)}).fetchone()

    if not row:
        return HTMLResponse("<div style='text-align:center;padding:60px;font-family:sans-serif'><h2>QR no válido</h2></div>", status_code=404)

    status_html = ""
    if row.used:
        status_html = "<div style='background:#ffebee;color:#c62828;padding:14px 20px;border-radius:8px;font-weight:700;text-align:center;margin-bottom:16px'>✗ Este regalo ya fue canjeado</div>"
    elif row.expires_at < datetime.now():
        status_html = "<div style='background:#ffebee;color:#c62828;padding:14px 20px;border-radius:8px;font-weight:700;text-align:center;margin-bottom:16px'>✗ Este regalo ha expirado</div>"
    else:
        status_html = f"<div style='background:#e8f5e9;color:#2e7d32;padding:14px 20px;border-radius:8px;font-weight:700;text-align:center;margin-bottom:16px'>✓ Válido · Expira hoy a las 23:59</div>"

    import io, base64
    voucher_url = f"{BASE_URL}/biz/{slug}/birthday/{token}"
    qr_img = qrcode.make(voucher_url)
    buf = io.BytesIO()
    qr_img.save(buf, format="PNG")
    qr_b64 = base64.b64encode(buf.getvalue()).decode()

    biz_gift_type    = getattr(biz, "birthday_gift_type", "discount") or "discount"
    biz_gift_product = getattr(biz, "birthday_gift_product", "") or ""
    if biz_gift_type == "product" and biz_gift_product:
        gift_display = f"""
  <div style="background:linear-gradient(135deg,#ff6b6b,#ffa07a);border-radius:14px;padding:20px;margin-bottom:20px">
    <div style="font-size:2rem;margin-bottom:4px">🎁</div>
    <div style="font-size:1.4rem;font-weight:900;color:#fff">{biz_gift_product}</div>
    <div style="color:rgba(255,255,255,.9);font-weight:700;font-size:.9rem;margin-top:4px">REGALO GRATIS</div>
  </div>"""
    else:
        gift_display = f"""
  <div style="background:linear-gradient(135deg,#ff6b6b,#ffa07a);border-radius:14px;padding:20px;margin-bottom:20px">
    <div style="font-size:3.5rem;font-weight:900;color:#fff">{row.discount_pct}%</div>
    <div style="color:rgba(255,255,255,.9);font-weight:700">DE DESCUENTO</div>
  </div>"""

    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🎂 Regalo Cumpleaños · {biz.name}</title>
<style>body{{margin:0;font-family:system-ui,sans-serif;background:#fff8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px;box-sizing:border-box}}
.card{{background:#fff;border-radius:20px;padding:32px 24px;max-width:380px;width:100%;box-shadow:0 8px 40px rgba(0,0,0,.12);text-align:center}}
</style></head>
<body><div class="card">
  <div style="font-size:3rem">🎂</div>
  <h1 style="font-size:1.4rem;color:#e65100;margin:8px 0">¡Feliz Cumpleaños, {row.first_name or 'Cliente'}!</h1>
  <p style="color:#888;font-size:.85rem;margin-bottom:20px">Tu regalo de <strong>{biz.name}</strong></p>
  {status_html}
  {gift_display}
  <p style="color:#555;font-size:.82rem;margin-bottom:16px">Muestra este QR al empleado para canjear tu regalo</p>
  <img src="data:image/png;base64,{qr_b64}" style="width:200px;height:200px;border-radius:10px;border:2px solid #f0f0f0;padding:6px">
  <p style="color:#bbb;font-size:.72rem;margin-top:16px">Un solo uso · Válido solo hoy</p>
</div></body></html>""")


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
                             headers={"Content-Disposition": "attachment; filename=zubcard_clientes.csv"})


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
}

@app.get("/api/admin/config")
def get_config(pin: str = "", slug: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = None  # ensure biz is always defined regardless of slug path
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
                    "INSERT INTO card_config (config, business_id, updated_at) "
                    "SELECT '{}', :bid, NOW() WHERE NOT EXISTS "
                    "(SELECT 1 FROM card_config WHERE business_id=:bid)"
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
def email_preview(card_id: str, pin: str = Query(""), db: Session = Depends(get_db)):
    """Preview the welcome email in browser — requires admin PIN."""
    verify_pin(pin, db)
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()
    name = customer.first_name if customer else "Cliente"
    card_url = f"{BASE_URL}/card/{card_id}"
    ref_code = get_or_create_referral_code(card_id, db)
    ref_url  = f"{BASE_URL}/register?ref={ref_code}"
    _biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first() if customer else None
    _prog = db.query(models.CardProgram).filter(models.CardProgram.business_id == _biz.id).first() if _biz else None
    html = render_welcome_email(name, card_url, card.stamps or 0, ref_code, ref_url,
                                **_prog_email_kwargs(_prog, _biz))
    return HTMLResponse(content=html)


@app.get("/email-preview-birthday/{card_id}", response_class=HTMLResponse)
def email_preview_birthday(card_id: str, pin: str = Query(""), db: Session = Depends(get_db)):
    """Preview the birthday email in browser — requires admin PIN."""
    verify_pin(pin, db)
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
        subject = f"¡Feliz Cumpleaños, {name}! 🎂"
    else:
        ref_code = get_or_create_referral_code(card_id, db)
        ref_url  = f"{BASE_URL}/register?ref={ref_code}"
        _biz2 = db.query(models.Business).filter(models.Business.id == customer.business_id).first() if customer.business_id else None
        _prog2 = db.query(models.CardProgram).filter(models.CardProgram.business_id == _biz2.id).first() if _biz2 else None
        html    = render_welcome_email(name, card_url, card.stamps or 0, ref_code, ref_url,
                                       **_prog_email_kwargs(_prog2, _biz2))
        subject = "¡Bienvenido/a! 🎉"
    # Per-business email branding
    biz_from_name = ""
    biz_reply_to  = ""
    if customer.business_id:
        biz = db.query(models.Business).filter(models.Business.id == customer.business_id).first()
        if biz:
            biz_from_name = biz.email_from_name or biz.name or ""
            biz_reply_to  = biz.email_reply_to  or ""
    sent = send_email(customer.email, subject, html, from_name=biz_from_name, reply_to=biz_reply_to)
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
            _biz3 = db.query(models.Business).filter(models.Business.id == cust.business_id).first() if cust.business_id else None
            _prog3 = db.query(models.CardProgram).filter(models.CardProgram.business_id == _biz3.id).first() if _biz3 else None
            html    = render_welcome_email(cust.first_name or "Cliente",
                                           f"{BASE_URL}/card/{card.id}",
                                           card.stamps or 0, ref_code, ref_url,
                                           **_prog_email_kwargs(_prog3, _biz3))
            if send_email(cust.email, "¡Tu tarjeta de fidelización te espera! 🎉", html):
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


async def _send_apns_campaign(db: Session, business_id: str, title: str, message: str, segment: str = "all") -> dict:
    """Send APNs push campaign to all wallet-registered devices for a business.
    Strategy: save the campaign message in businesses.promo_message, then send
    a silent background push. When Wallet wakes up it fetches the updated pass
    which now contains the new promo_message in a field with changeMessage set.
    iOS then shows the notification: '[biz] [message]'.
    """
    # 1. Store the campaign message so the pass update endpoint can embed it
    # Append a short timestamp to ensure the value ALWAYS changes (so changeMessage fires every time)
    from datetime import datetime as _dt
    combined_raw = f"{title}: {message}" if title and message else (title or message)
    ts_suffix = _dt.now().strftime(" · %d/%m %H:%M")
    combined = (combined_raw + ts_suffix)[:120]
    try:
        db.execute(text(
            "UPDATE businesses SET promo_message=:msg WHERE id=:bid"
        ), {"msg": combined, "bid": str(business_id)})
        # Touch updated_at on ALL loyalty_cards for this business so that
        # wallet_list_updatable_passes returns them when Wallet polls with passesUpdatedSince
        db.execute(text(
            "UPDATE loyalty_cards SET updated_at = NOW() "
            "WHERE customer_id IN (SELECT id FROM customers WHERE business_id = :bid)"
        ), {"bid": str(business_id)})
        db.commit()
        print(f"✅ promo_message saved: '{combined}' for business {business_id}")
    except Exception as e:
        print(f"Could not save promo_message: {e}")

    q = (
        "SELECT wd.push_token, c.first_name FROM wallet_devices wd "
        "JOIN loyalty_cards lc ON lc.id = wd.card_id "
        "JOIN customers c ON c.id = lc.customer_id "
        "WHERE c.business_id = :bid"
    )
    params: dict = {"bid": str(business_id)}
    if segment == "active":
        q += " AND lc.stamps > 0"
    elif segment == "near_reward":
        q += " AND lc.stamps >= (SELECT stamps_per_reward - 2 FROM businesses WHERE id=:bid2)"
        params["bid2"] = str(business_id)
    elif segment == "inactive":
        q += " AND lc.updated_at < NOW() - INTERVAL '30 days'"
    elif segment.startswith("min_stamps:"):
        try:
            min_s = int(segment.split(":")[1])
            q += " AND lc.stamps >= :min_s"
            params["min_s"] = min_s
        except Exception:
            pass
    rows = db.execute(text(q), params).fetchall()
    print(f"Campaign: found {len(rows)} wallet device(s) to notify for business {business_id}")
    sent = failed = 0
    for row in rows:
        push_token = row[0]
        try:
            # Use priority=10 for campaign pushes so iOS delivers immediately (not throttled)
            ok = await _apns_send(push_token, payload={}, push_type="background", priority=10)
            if ok:
                sent += 1
                print(f"  ✅ APNs accepted for token ...{push_token[-8:]}")
            else:
                failed += 1
                print(f"  ❌ APNs rejected for token ...{push_token[-8:]}")
        except Exception as ex:
            print(f"Campaign APNs error: {ex}")
            failed += 1
    print(f"Campaign APNs result: {sent} sent, {failed} failed out of {len(rows)} wallet devices")
    return {"sent": sent, "failed": failed, "total_wallet_devices": len(rows)}


def _send_push_to_subscriptions(subs_rows, title: str, message: str, url: str = "/") -> dict:
    """Send a web push notification to a list of (endpoint, p256dh, auth) rows."""
    if not VAPID_PUBLIC or not VAPID_PRIVATE:
        return {"sent": 0, "failed": 0, "dry_run": True,
                "note": "Configura VAPID_PUBLIC_KEY y VAPID_PRIVATE_KEY en Railway para activar push."}
    try:
        from pywebpush import webpush, WebPushException  # type: ignore
    except ImportError:
        return {"sent": 0, "failed": 0, "dry_run": True,
                "note": "pywebpush no instalado — despliega de nuevo para activar."}
    import json as _json
    payload = _json.dumps({"title": title, "body": message, "icon": "/static/icon-192.png", "url": url})
    vapid_claims = {"sub": "mailto:hola@zubcard.com"}
    sent = failed = 0
    for row in subs_rows:
        endpoint, p256dh, auth_key = row[0], row[1], row[2]
        try:
            webpush(
                subscription_info={"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth_key}},
                data=payload,
                vapid_private_key=VAPID_PRIVATE,
                vapid_claims=vapid_claims,
            )
            sent += 1
        except WebPushException as e:
            failed += 1
            print(f"Push failed for {endpoint[:40]}…: {e}")
        except Exception as e:
            failed += 1
            print(f"Push error: {e}")
    return {"sent": sent, "failed": failed, "dry_run": False}


@app.post("/api/admin/push/send")
async def admin_push_send(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    title   = body.get("title", "ZubCard")
    message = body.get("message", "")
    url     = body.get("url", "/")
    rows = db.execute(text(
        "SELECT endpoint, p256dh, auth FROM push_subscriptions"
    )).fetchall()
    result = _send_push_to_subscriptions(rows, title, message, url)
    result["total_subscribers"] = len(rows)
    result["message"] = f"Push '{title}' → {result['sent']} enviados, {result['failed']} fallidos"
    return result


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
  const title = data.title || 'ZubCard';
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


# ── /tier endpoint kept for backward compat — VIP tiers removed ──────────────
@app.get("/api/cards/{card_id}/tier")
def get_card_tier(card_id: str, db: Session = Depends(get_db)):
    card = get_card_or_404(card_id, db)
    return {
        "tier": None,
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
    """Returns loyalty card info and program config — no VIP tiers"""
    card = get_card_or_404(card_id, db)
    customer = db.query(models.Customer).filter(models.Customer.id == card.customer_id).first()

    biz_id = customer.business_id if customer else None
    biz = db.query(models.Business).filter(models.Business.id == biz_id).first() if biz_id else None

    # Load config for this business
    config = {}
    if biz_id:
        row = db.execute(
            text("SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"),
            {"bid": str(biz_id)}
        ).fetchone()
        if row:
            config = json.loads(row[0]) if row[0] else {}

    programa = {**DEFAULT_CONFIG.get("programa", {}), **config.get("programa", {})}
    stamps_per_reward = biz.stamps_per_reward if biz else programa.get("stamps_per_reward", 10)
    reward_name = programa.get("reward_name", "Premio")

    stamps_on_card = card.stamps or 0
    stamps_to_next_reward = max(0, stamps_per_reward - stamps_on_card)
    total = card.total_stamps or 0

    # Member number scoped to this business
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
        "program_name":       biz.card_title if biz else "Tarjeta de Fidelización",
        "reward_name":        reward_name,
        "stamps_per_reward":  stamps_per_reward,
        "stamps_on_card":     stamps_on_card,
        "stamps_to_next_reward": stamps_to_next_reward,
        "total_stamps":       total,
        "award_balance":      card.award_balance or 0,
        "rewards_redeemed":   card.rewards_redeemed or 0,
        "member_number":      member_number,
        "member_since":       customer.created_at.strftime("%B %Y") if customer and customer.created_at else "",
    }


@app.get("/api/push/vapid-public")
def get_vapid_public():
    return {"key": VAPID_PUBLIC or None}


@app.get("/api/biz/{slug}/vapid-public-key")
def get_biz_vapid_public(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Return the VAPID public key for the given business (frontend VAPID status check)."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    if not VAPID_PUBLIC:
        raise HTTPException(status_code=404, detail="VAPID no configurado")
    return {"public_key": VAPID_PUBLIC}


@app.get("/api/biz/{slug}/push-subscriptions/count")
def get_push_sub_count(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Return the number of active web push subscriptions for this business."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    row = db.execute(
        text(
            "SELECT COUNT(*) FROM push_subscriptions ps "
            "JOIN loyalty_cards lc ON lc.id = ps.card_id "
            "JOIN customers c ON c.id = lc.customer_id "
            "WHERE c.business_id = :bid"
        ),
        {"bid": str(biz.id)},
    ).fetchone()
    return {"count": row[0] if row else 0}


def _photon_to_nominatim(features: list) -> list:
    """Convert Photon GeoJSON features to Nominatim-style dicts."""
    results = []
    for f in features:
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [None, None])
        if not coords[0]:
            continue
        parts = []
        if props.get("name"):      parts.append(props["name"])
        if props.get("housenumber"): parts.append(props["housenumber"])
        if props.get("street"):    parts.append(props["street"])
        if props.get("postcode"):  parts.append(props["postcode"])
        if props.get("city"):      parts.append(props["city"])
        if props.get("state"):     parts.append(props["state"])
        if props.get("country"):   parts.append(props["country"])
        display = ", ".join(p for p in parts if p)
        results.append({
            "lat": str(coords[1]),
            "lon": str(coords[0]),
            "display_name": display,
        })
    return results


@app.get("/api/geo/search")
async def geo_search(q: str = "", limit: int = 5):
    """Proxy geocoding search — uses Photon (better Spain coverage) with Nominatim fallback."""
    if not q.strip():
        raise HTTPException(status_code=400, detail="q is required")
    headers_nominatim = {"User-Agent": "ZubCard/1.0 (hola@zubcard.com)", "Accept-Language": "es"}
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            # 1st try: Photon (komoot) — great street-level coverage in Spain
            r1 = await client.get(
                "https://photon.komoot.io/api/",
                params={"q": q, "limit": limit, "bbox": "-9.5,35.5,4.5,44.0"},
                headers={"User-Agent": "ZubCard/1.0 (hola@zubcard.com)"},
            )
            if r1.status_code == 200:
                geojson = r1.json()
                features = geojson.get("features", [])
                if features:
                    return _photon_to_nominatim(features)

            # Fallback: Nominatim with countrycodes=es
            r2 = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"format": "json", "q": q, "limit": limit, "countrycodes": "es"},
                headers=headers_nominatim,
            )
            data = r2.json()
            if data:
                return data

            # Last resort: Nominatim worldwide
            r3 = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"format": "json", "q": q, "limit": limit},
                headers=headers_nominatim,
            )
            return r3.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Geocoding error: {e}")


@app.get("/api/geo/reverse")
async def geo_reverse(lat: float = 0.0, lng: float = 0.0):
    """Proxy reverse geocoding to Nominatim (server-side, with proper User-Agent)."""
    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers={"User-Agent": "ZubCard/1.0 (hola@zubcard.com)", "Accept-Language": "es"})
            return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Reverse geocoding error: {e}")



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
    """Register a new business on ZubCard with email + password (no PIN required from user)."""
    body     = await request.json()
    name        = (body.get("name") or "").strip()
    email       = (body.get("email") or "").strip().lower()
    password    = str(body.get("password") or "").strip()
    industry    = body.get("industry", "other")
    description = (body.get("description") or "").strip()
    address     = (body.get("address") or "").strip()
    latitude    = body.get("latitude")
    longitude   = body.get("longitude")

    if not name:
        raise HTTPException(status_code=400, detail="El nombre del negocio es obligatorio")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Email inválido")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="La contraseña debe tener al menos 8 caracteres")

    if db.query(models.Business).filter(models.Business.email == email).first():
        raise HTTPException(status_code=409, detail="Ya existe una cuenta con ese email. ¿Quieres iniciar sesión?")

    # Hash password with bcrypt
    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    # Auto-generate internal API PIN (random 6 digits — user never sees/sets this)
    random_pin = "".join(secrets.choice(string.digits) for _ in range(6))

    # Email confirmation token
    confirm_token = secrets.token_urlsafe(32)

    # Generate unique slug
    base_slug = generate_slug(name)
    slug = base_slug
    counter = 1
    while db.query(models.Business).filter(models.Business.slug == slug).first():
        slug = f"{base_slug}{counter}"
        counter += 1

    business = models.Business(
        name                = name,
        slug                = slug,
        email               = email,
        hashed_password     = hashed_pw,
        email_confirmed     = False,
        email_confirm_token = confirm_token,
        admin_pin           = random_pin,
        api_key             = generate_api_key(),
        industry            = industry,
        description         = description or None,
        address             = address or None,
        latitude            = float(latitude) if latitude else None,
        longitude           = float(longitude) if longitude else None,
        plan                = "free",
    )
    db.add(business)
    db.commit()
    db.refresh(business)

    # Create default card config
    db.execute(text(
        "INSERT INTO card_config (config, business_id, updated_at) "
        "SELECT '{}', :bid, NOW() WHERE NOT EXISTS "
        "(SELECT 1 FROM card_config WHERE business_id=:bid)"
    ), {"bid": str(business.id)})
    db.commit()

    # ── Stripe checkout inmediato (14-day trial, tarjeta requerida) ──────────
    checkout_url = None
    if STRIPE_SECRET_KEY and STRIPE_PRICE_ID_PRO:
        try:
            stripe_customer = _stripe_sdk.Customer.create(
                email=email,
                name=name,
                metadata={"biz_id": str(business.id), "biz_slug": slug},
            )
            db.execute(text("UPDATE businesses SET stripe_customer_id=:cid WHERE id=:bid"),
                       {"cid": stripe_customer.id, "bid": str(business.id)})
            db.commit()
            stripe_session = _stripe_sdk.checkout.Session.create(
                customer=stripe_customer.id,
                mode="subscription",
                line_items=[{"price": STRIPE_PRICE_ID_PRO, "quantity": 1}],
                success_url=f"{BASE_URL}/api/app/checkout-success?token={confirm_token}&slug={slug}",
                cancel_url=f"{BASE_URL}/app/register?checkout_cancelled=1&email={email}",
                subscription_data={
                    "metadata": {"biz_id": str(business.id), "biz_slug": slug},
                    "trial_period_days": 14,
                },
                allow_promotion_codes=True,
                customer_update={"name": "auto", "address": "auto"},
                tax_id_collection={"enabled": True},
                billing_address_collection="required",
                locale="es",
            )
            checkout_url = stripe_session.url
        except Exception as e:
            print(f"⚠️ Stripe checkout creation failed at registration: {e}")

    # ── Email de confirmación (backup si el usuario no completa el pago) ────
    confirm_url = f"{BASE_URL}/api/app/confirm-email?token={confirm_token}"
    try:
        email_sent = send_email(
            to_email=email,
            subject="Confirma tu cuenta en ZubCard",
            html_body=f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px">
  <h2 style="color:#26170c;font-size:1.4rem;margin-bottom:8px">¡Bienvenido a ZubCard, {name}! 🎉</h2>
  <p style="color:#6b5c54;line-height:1.7;margin-bottom:24px">
    Para activar tu cuenta y acceder a tu panel de administración, confirma tu email haciendo clic en el botón de abajo.
  </p>
  <a href="{confirm_url}" style="display:inline-block;background:#26170c;color:#fff;padding:13px 28px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.95rem">
    ✅ Confirmar mi cuenta
  </a>
  <p style="color:#a08d83;font-size:.82rem;margin-top:28px;line-height:1.6">
    Si no creaste esta cuenta, ignora este mensaje.<br>
    El enlace expira en 24 horas.
  </p>
</div>"""
        )
    except Exception:
        email_sent = False

    return {
        "message":     "Cuenta creada. Completa tu prueba gratuita de 14 días.",
        "email_sent":  email_sent,
        "slug":        slug,
        "name":        name,
        "checkout_url": checkout_url,   # frontend redirige aquí inmediatamente
    }


@app.get("/api/app/confirm-email")
async def confirm_email(token: str = "", db: Session = Depends(get_db)):
    """Confirm email address from the link sent after registration."""
    if not token:
        raise HTTPException(status_code=400, detail="Token requerido")
    biz = db.query(models.Business).filter(
        models.Business.email_confirm_token == token
    ).first()
    if not biz:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;text-align:center;padding:60px'>"
            "<h2 style='color:#c62828'>Enlace inválido o ya utilizado</h2>"
            "<p><a href='/app/login'>Ir al inicio de sesión</a></p></body></html>",
            status_code=400
        )
    # Mark confirmed
    db.execute(text(
        "UPDATE businesses SET email_confirmed=TRUE, email_confirm_token=NULL WHERE id=:bid"
    ), {"bid": str(biz.id)})
    db.commit()
    # Redirect to dashboard without putting the PIN in the URL.
    # We pass the PIN via a short-lived HttpOnly=False cookie (JS needs to read it)
    # so it never appears in server logs, browser history, or referrer headers.
    response = RedirectResponse(f"/biz/{biz.slug}/dashboard?confirmed=1", status_code=302)
    response.set_cookie(
        "_zc_boot",
        biz.admin_pin,
        max_age=90,          # expires in 90 s — enough for page load
        httponly=False,      # JS must read it to auto-login
        samesite="strict",
        path=f"/biz/{biz.slug}/dashboard",
    )
    return response


@app.get("/api/app/checkout-success")
async def checkout_success_redirect(token: str = "", slug: str = "", db: Session = Depends(get_db)):
    """After Stripe checkout completes: confirm email + auto-login to dashboard."""
    biz = None
    if token:
        biz = db.query(models.Business).filter(
            models.Business.email_confirm_token == token
        ).first()
        if biz:
            db.execute(text(
                "UPDATE businesses SET email_confirmed=TRUE, email_confirm_token=NULL WHERE id=:bid"
            ), {"bid": str(biz.id)})
            db.commit()
    if not biz and slug:
        biz = get_business_by_slug(slug, db)
    if not biz:
        return RedirectResponse("/app/login?checkout_ok=1", status_code=302)
    target_slug = biz.slug
    response = RedirectResponse(f"/biz/{target_slug}/dashboard?stripe_success=1", status_code=302)
    response.set_cookie(
        "_zc_boot",
        biz.admin_pin,
        max_age=90,
        httponly=False,
        samesite="strict",
        path=f"/biz/{target_slug}/dashboard",
    )
    return response


@app.post("/api/app/resend-confirm")
async def resend_confirm(request: Request, db: Session = Depends(get_db)):
    """Resend the email confirmation link to a registered-but-unconfirmed business."""
    body  = await request.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    biz = db.query(models.Business).filter(models.Business.email == email).first()
    if not biz:
        # Don't reveal whether the email exists
        return {"message": "Si existe una cuenta con ese email, recibirás el enlace en breve."}
    if biz.email_confirmed:
        return {"message": "Esta cuenta ya está confirmada. Puedes iniciar sesión."}

    # Regenerate token
    confirm_token = secrets.token_urlsafe(32)
    db.execute(text(
        "UPDATE businesses SET email_confirm_token=:tok WHERE id=:bid"
    ), {"tok": confirm_token, "bid": str(biz.id)})
    db.commit()

    confirm_url = f"{BASE_URL}/api/app/confirm-email?token={confirm_token}"
    try:
        send_email(
            to_email=email,
            subject="Confirma tu cuenta en ZubCard",
            html_body=f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px">
  <h2 style="color:#26170c;font-size:1.3rem;margin-bottom:8px">Confirma tu cuenta en ZubCard</h2>
  <p style="color:#6b5c54;line-height:1.7;margin-bottom:24px">Haz clic en el enlace para activar tu cuenta:</p>
  <a href="{confirm_url}" style="display:inline-block;background:#26170c;color:#fff;padding:13px 28px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.95rem">
    ✅ Confirmar mi cuenta
  </a>
  <p style="color:#a08d83;font-size:.82rem;margin-top:28px">El enlace expira en 24 horas.</p>
</div>"""
        )
    except Exception:
        pass

    return {"message": "Si existe una cuenta con ese email, recibirás el enlace en breve."}


@app.get("/app/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request):
    """Simple forgot password page."""
    return HTMLResponse("""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Recuperar contraseña – ZubCard</title>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:linear-gradient(135deg,#fff8f5,#ffeade);font-family:'Manrope',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.c{background:#fff;border-radius:1.25rem;padding:44px 40px;width:100%;max-width:420px;box-shadow:0 8px 40px rgba(38,23,12,.14)}
.brand{font-family:'Bebas Neue',sans-serif;font-size:1.9rem;color:#26170c;margin-bottom:24px;text-align:center}
.brand span{color:#785a00}
h2{font-size:1.1rem;font-weight:700;color:#26170c;margin-bottom:8px;text-align:center}
p{color:#a08d83;font-size:.88rem;line-height:1.7;text-align:center;margin-bottom:24px}
label{display:block;font-size:.72rem;font-weight:800;letter-spacing:1.5px;text-transform:uppercase;color:#a08d83;margin-bottom:5px;margin-top:14px}
input{width:100%;padding:11px 14px;border:none;border-radius:.375rem;font-size:.93rem;outline:none;background:#fff8f5;color:#26170c;font-family:'Manrope',sans-serif}
input:focus{box-shadow:0 0 0 2.5px #ffca48}
.btn{width:100%;background:#26170c;color:#fff;border:none;padding:13px;border-radius:.375rem;font-size:.9rem;font-weight:800;cursor:pointer;margin-top:18px;font-family:'Manrope',sans-serif}
.btn:disabled{background:#a08d83;cursor:not-allowed}
.msg{padding:10px 14px;border-radius:.375rem;font-size:.83rem;margin-top:14px;display:none;line-height:1.5}
.msg.ok{background:#e8f5e9;color:#2e7d32;border-left:3px solid #2e7d32}
.msg.err{background:#fbe9e7;color:#c62828;border-left:3px solid #c62828}
.back{text-align:center;font-size:.82rem;color:#a08d83;margin-top:18px}
.back a{color:#785a00;font-weight:700;text-decoration:none}
</style></head><body>
<div class="c">
  <div class="brand">Zub<span>Card</span></div>
  <h2>Recuperar contraseña</h2>
  <p>Introduce tu email y te enviaremos un enlace para restablecer tu contraseña.</p>
  <label>Email</label>
  <input type="email" id="fp-email" placeholder="tu@negocio.com" autocomplete="email">
  <div class="msg" id="fp-msg"></div>
  <button class="btn" id="fp-btn" onclick="sendReset()">Enviar enlace →</button>
  <div class="back"><a href="/app/login">← Volver al inicio de sesión</a></div>
</div>
<script>
async function sendReset(){
  const email=document.getElementById('fp-email').value.trim();
  const btn=document.getElementById('fp-btn');
  const msg=document.getElementById('fp-msg');
  if(!email){msg.className='msg err';msg.textContent='Introduce tu email';msg.style.display='block';return;}
  btn.disabled=true;btn.textContent='Enviando…';
  try{
    const r=await fetch('/api/app/reset-password-request',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email})});
    msg.className='msg ok';
    msg.textContent='Si existe una cuenta con ese email, recibirás el enlace en breve. Revisa tu bandeja de entrada.';
    msg.style.display='block';
    btn.textContent='Enviado ✅';
  }catch(e){
    msg.className='msg err';msg.textContent='Error de conexión. Inténtalo de nuevo.';msg.style.display='block';
    btn.disabled=false;btn.textContent='Enviar enlace →';
  }
}
</script></body></html>""")


@app.post("/api/app/reset-password-request")
async def reset_password_request(request: Request, db: Session = Depends(get_db)):
    """Send a password reset link by email."""
    body  = await request.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    biz = db.query(models.Business).filter(models.Business.email == email).first()
    # Always respond the same way to prevent email enumeration
    if biz and biz.hashed_password:
        reset_token = secrets.token_urlsafe(32)
        db.execute(text(
            "UPDATE businesses SET email_confirm_token=:tok WHERE id=:bid"
        ), {"tok": f"reset_{reset_token}", "bid": str(biz.id)})
        db.commit()
        reset_url = f"{BASE_URL}/app/reset-password?token={reset_token}"
        try:
            send_email(
                to_email=email,
                subject="Restablece tu contraseña en ZubCard",
                html_body=f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px">
  <h2 style="color:#26170c;margin-bottom:8px">Restablecer contraseña</h2>
  <p style="color:#6b5c54;line-height:1.7;margin-bottom:24px">
    Haz clic en el enlace para crear una nueva contraseña. Válido durante 1 hora.
  </p>
  <a href="{reset_url}" style="display:inline-block;background:#26170c;color:#fff;padding:13px 28px;border-radius:6px;text-decoration:none;font-weight:700">
    🔑 Restablecer contraseña
  </a>
  <p style="color:#a08d83;font-size:.82rem;margin-top:28px">Si no solicitaste este cambio, ignora este mensaje.</p>
</div>"""
            )
        except Exception:
            pass
    return {"message": "Si existe una cuenta con ese email, recibirás el enlace en breve."}


@app.get("/app/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request, token: str = ""):
    """Password reset form."""
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Nueva contraseña – ZubCard</title>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:linear-gradient(135deg,#fff8f5,#ffeade);font-family:'Manrope',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}
.c{{background:#fff;border-radius:1.25rem;padding:44px 40px;width:100%;max-width:420px;box-shadow:0 8px 40px rgba(38,23,12,.14)}}
.brand{{font-family:'Bebas Neue',sans-serif;font-size:1.9rem;color:#26170c;margin-bottom:24px;text-align:center}}
.brand span{{color:#785a00}}
h2{{font-size:1.1rem;font-weight:700;color:#26170c;margin-bottom:20px;text-align:center}}
label{{display:block;font-size:.72rem;font-weight:800;letter-spacing:1.5px;text-transform:uppercase;color:#a08d83;margin-bottom:5px;margin-top:14px}}
.iw{{position:relative}}
input{{width:100%;padding:11px 14px;border:none;border-radius:.375rem;font-size:.93rem;outline:none;background:#fff8f5;color:#26170c;font-family:'Manrope',sans-serif}}
input:focus{{box-shadow:0 0 0 2.5px #ffca48}}
.tpw{{position:absolute;right:12px;top:50%;transform:translateY(-50%);cursor:pointer;color:#a08d83;font-size:1.05rem;background:none;border:none;padding:0}}
.btn{{width:100%;background:#26170c;color:#fff;border:none;padding:13px;border-radius:.375rem;font-size:.9rem;font-weight:800;cursor:pointer;margin-top:18px;font-family:'Manrope',sans-serif}}
.btn:disabled{{background:#a08d83;cursor:not-allowed}}
.msg{{padding:10px 14px;border-radius:.375rem;font-size:.83rem;margin-top:14px;display:none}}
.msg.ok{{background:#e8f5e9;color:#2e7d32;border-left:3px solid #2e7d32}}
.msg.err{{background:#fbe9e7;color:#c62828;border-left:3px solid #c62828}}
</style></head><body>
<div class="c">
  <div class="brand">Zub<span>Card</span></div>
  <h2>Crea una nueva contraseña</h2>
  <label>Nueva contraseña</label>
  <div class="iw">
    <input type="password" id="np1" placeholder="Mínimo 8 caracteres" autocomplete="new-password">
    <button type="button" class="tpw" onclick="t('np1',this)">👁</button>
  </div>
  <label>Confirmar contraseña</label>
  <div class="iw">
    <input type="password" id="np2" placeholder="Repite la contraseña" autocomplete="new-password">
    <button type="button" class="tpw" onclick="t('np2',this)">👁</button>
  </div>
  <div class="msg" id="rp-msg"></div>
  <button class="btn" id="rp-btn" onclick="doReset()">Guardar nueva contraseña →</button>
</div>
<script>
const TOKEN='{token}';
function t(id,btn){{const i=document.getElementById(id);i.type=i.type==='password'?'text':'password';btn.textContent=i.type==='password'?'👁':'🙈';}}
async function doReset(){{
  const pw=document.getElementById('np1').value;
  const pw2=document.getElementById('np2').value;
  const msg=document.getElementById('rp-msg');
  const btn=document.getElementById('rp-btn');
  msg.style.display='none';
  if(pw.length<8){{msg.className='msg err';msg.textContent='Mínimo 8 caracteres';msg.style.display='block';return;}}
  if(pw!==pw2){{msg.className='msg err';msg.textContent='Las contraseñas no coinciden';msg.style.display='block';return;}}
  btn.disabled=true;btn.textContent='Guardando…';
  try{{
    const r=await fetch('/api/app/reset-password',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{token:TOKEN,password:pw}})}});
    const d=await r.json();
    if(r.ok){{msg.className='msg ok';msg.textContent='Contraseña actualizada. Redirigiendo…';msg.style.display='block';setTimeout(()=>window.location.href='/app/login',2000);}}
    else{{msg.className='msg err';msg.textContent=d.detail||'Error';msg.style.display='block';btn.disabled=false;btn.textContent='Guardar nueva contraseña →';}}
  }}catch(e){{msg.className='msg err';msg.textContent='Error de conexión';msg.style.display='block';btn.disabled=false;btn.textContent='Guardar nueva contraseña →';}}
}}
</script></body></html>""")


@app.post("/api/app/reset-password")
async def reset_password_apply(request: Request, db: Session = Depends(get_db)):
    """Apply the new password after clicking the reset link."""
    body     = await request.json()
    token    = (body.get("token") or "").strip()
    password = str(body.get("password") or "").strip()

    if not token or len(password) < 8:
        raise HTTPException(status_code=400, detail="Token y contraseña (min. 8 caracteres) requeridos")

    biz = db.query(models.Business).filter(
        models.Business.email_confirm_token == f"reset_{token}"
    ).first()
    if not biz:
        raise HTTPException(status_code=400, detail="Enlace inválido o ya utilizado")

    hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    db.execute(text(
        "UPDATE businesses SET hashed_password=:pw, email_confirm_token=NULL, email_confirmed=TRUE WHERE id=:bid"
    ), {"pw": hashed_pw, "bid": str(biz.id)})
    db.commit()
    return {"message": "Contraseña actualizada correctamente"}


@app.post("/api/app/login")
async def login_business(request: Request, db: Session = Depends(get_db)):
    """Login to business account with email + password (bcrypt)."""
    body     = await request.json()
    email    = (body.get("email") or "").strip().lower()
    password = str(body.get("password") or "").strip()

    biz = db.query(models.Business).filter(
        models.Business.email == email,
        models.Business.active == True
    ).first()

    # Must exist and have a password (Google-only accounts have no password)
    if not biz or not biz.hashed_password:
        raise HTTPException(status_code=401, detail="Email o contraseña incorrectos")

    # Verify password
    try:
        password_ok = bcrypt.checkpw(password.encode(), biz.hashed_password.encode())
    except Exception:
        password_ok = False

    if not password_ok:
        raise HTTPException(status_code=401, detail="Email o contraseña incorrectos")

    if not biz.email_confirmed:
        raise HTTPException(
            status_code=403,
            detail="Confirma tu email antes de iniciar sesión. Revisa tu bandeja de entrada."
        )

    return {
        "message":       "Login correcto",
        "slug":          biz.slug,
        "name":          biz.name,
        "plan":          biz.plan or "free",
        "pin":           biz.admin_pin,           # internal token for dashboard API calls
        "dashboard_url": f"/biz/{biz.slug}/dashboard",
    }


# ════════════════════════════════════════════════════════════════════════════════
# BUSINESS PROFILE — GET / PUT / CHANGE-PIN
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/biz/{slug}/profile")
def get_biz_profile(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Get business profile info (name, email, slug, industry, geo)"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    return {
        "name":          biz.name,
        "email":         biz.email,
        "slug":          biz.slug,
        "industry":      getattr(biz, "industry", "other"),
        "description":   getattr(biz, "description", None) or "",
        "plan":          getattr(biz, "plan", "pro"),
        "created_at":    str(biz.created_at) if hasattr(biz, "created_at") else None,
        # Geo-location fields
        "address":       getattr(biz, "address", None) or "",
        "latitude":      getattr(biz, "latitude", None),
        "longitude":     getattr(biz, "longitude", None),
        "geo_radius_m":  getattr(biz, "geo_radius_m", 300) or 300,
        "geo_push_msg":  getattr(biz, "geo_push_msg", "") or "¡Estás cerca! Visítanos y acumula sellos 🎉",
    }


# ════════════════════════════════════════════════════════════════════════════════
# GEO-LOCATION / ADDRESS — GET & PUT
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/biz/{slug}/geo")
def get_biz_geo(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Get geo/address config for push notifications"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    return {
        "address":      getattr(biz, "address", None) or "",
        "latitude":     getattr(biz, "latitude", None),
        "longitude":    getattr(biz, "longitude", None),
        "geo_radius_m": getattr(biz, "geo_radius_m", 300) or 300,
        "geo_push_msg": getattr(biz, "geo_push_msg", "") or "¡Estás cerca! Visítanos y acumula sellos 🎉",
    }


@app.put("/api/biz/{slug}/geo")
async def update_biz_geo(slug: str, request: Request, db: Session = Depends(get_db)):
    """Update geo/address for proximity push notifications"""
    body = await request.json()
    pin  = str(body.get("pin", "")).strip()
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")
    db.execute(text(
        "UPDATE businesses SET address=:addr, latitude=:lat, longitude=:lng, "
        "geo_radius_m=:radius, geo_push_msg=:msg WHERE slug=:slug"
    ), {
        "addr":   body.get("address", ""),
        "lat":    body.get("latitude"),
        "lng":    body.get("longitude"),
        "radius": int(body.get("geo_radius_m", 300)),
        "msg":    body.get("geo_push_msg", "¡Estás cerca! Visítanos y acumula sellos 🎉"),
        "slug":   slug,
    })
    db.commit()
    return {"status": "updated"}


@app.post("/api/biz/{slug}/geo/push-nearby")
async def geo_push_nearby(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """
    Send a proximity push notification to all customers of this business.
    - Apple Wallet users: APNs alert via wallet_devices (no opt-in required beyond adding card)
    - Android / browser users: web push via push_subscriptions (requires browser opt-in)
    Note: iOS users with the pass added to Wallet also get automatic geo alerts from the
    pass's locations[] field when they are physically near the store.
    """
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if pin != str(biz.admin_pin):
        raise HTTPException(status_code=403, detail="PIN incorrecto")

    push_msg = getattr(biz, "geo_push_msg", None) or "¡Estás cerca! Visítanos y acumula sellos 🎉"
    biz_name = biz.name or "ZubCard"

    # ── Apple Wallet (APNs) ──────────────────────────────────────────────────
    apns_result = await _send_apns_campaign(
        db,
        business_id=str(biz.id),
        title=biz_name,
        message=push_msg,
        segment="all",
    )

    # ── Android / browser web push ───────────────────────────────────────────
    web_sent = web_failed = 0
    web_rows = db.execute(text(
        "SELECT ps.endpoint, ps.p256dh, ps.auth "
        "FROM push_subscriptions ps "
        "JOIN loyalty_cards lc ON lc.id = ps.card_id "
        "JOIN customers c ON c.id = lc.customer_id "
        "WHERE c.business_id = :bid"
    ), {"bid": str(biz.id)}).fetchall()

    if web_rows and VAPID_PUBLIC and VAPID_PRIVATE:
        try:
            from pywebpush import webpush, WebPushException  # type: ignore
            import json as _json
            payload = _json.dumps({"title": biz_name, "body": push_msg, "icon": "/static/icon-192.png"})
            vapid_claims = {"sub": f"mailto:{biz.email}"}
            for endpoint, p256dh, auth_key in web_rows:
                try:
                    webpush(
                        subscription_info={"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth_key}},
                        data=payload,
                        vapid_private_key=VAPID_PRIVATE,
                        vapid_claims=vapid_claims,
                    )
                    web_sent += 1
                except Exception:
                    web_failed += 1
        except ImportError:
            pass

    total_sent = apns_result["sent"] + web_sent
    return {
        "message": f"Notificación enviada a {total_sent} cliente(s)",
        "sent": total_sent,
        "apple_wallet": {"sent": apns_result["sent"], "failed": apns_result["failed"], "devices": apns_result["total_wallet_devices"]},
        "android_web": {"sent": web_sent, "failed": web_failed, "subscribers": len(web_rows)},
    }


# ════════════════════════════════════════════════════════════════════════════════
# GOOGLE REVIEWS — GET & PUT
# ════════════════════════════════════════════════════════════════════════════════
@app.get("/api/biz/{slug}/reviews-config")
def get_reviews_config(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Get Google Reviews configuration for this business"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return {
        "google_review_url":    getattr(biz, "google_review_url", "") or "",
        "review_trigger_stamps": getattr(biz, "review_trigger_stamps", 0) or 0,
    }


@app.put("/api/biz/{slug}/reviews-config")
async def update_reviews_config(slug: str, request: Request, db: Session = Depends(get_db)):
    """Update Google Reviews configuration"""
    body = await request.json()
    pin  = str(body.get("pin", "")).strip()
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text(
        "UPDATE businesses SET google_review_url=:url, review_trigger_stamps=:trigger WHERE slug=:slug"
    ), {
        "url":     (body.get("google_review_url") or "").strip(),
        "trigger": int(body.get("review_trigger_stamps") or 0),
        "slug":    slug,
    })
    db.commit()
    return {"status": "updated"}


@app.get("/api/biz/{slug}/birthday-config")
def get_birthday_config(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Get birthday gift configuration for this business"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return {
        "birthday_gift_type":         getattr(biz, "birthday_gift_type", "discount") or "discount",
        "birthday_gift_product":      getattr(biz, "birthday_gift_product", "") or "",
        "birthday_discount_pct":      20,
        "birthday_email_intro":       getattr(biz, "birthday_email_intro", "") or "",
        "birthday_email_header_color":getattr(biz, "birthday_email_header_color", "") or "",
        "birthday_email_accent_color":getattr(biz, "birthday_email_accent_color", "") or "",
        "birthday_email_banner_url":  getattr(biz, "birthday_email_banner_url", "") or "",
        "birthday_email_footer_text": getattr(biz, "birthday_email_footer_text", "") or "",
        "biz_primary_color":          getattr(biz, "primary_color", "#26170c") or "#26170c",
        "biz_accent_color":           getattr(biz, "accent_color", "#ffca48") or "#ffca48",
        "biz_logo_url":               getattr(biz, "logo_url", "") or "",
        "biz_name":                   biz.name or "",
    }


@app.put("/api/biz/{slug}/birthday-config")
async def update_birthday_config(slug: str, request: Request, db: Session = Depends(get_db)):
    """Update birthday gift configuration"""
    body = await request.json()
    pin  = str(body.get("pin", "")).strip()
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    gift_type    = (body.get("birthday_gift_type") or "discount").strip()
    gift_product = (body.get("birthday_gift_product") or "").strip()
    email_intro  = (body.get("birthday_email_intro") or "").strip()
    header_color = (body.get("birthday_email_header_color") or "").strip()
    accent_color = (body.get("birthday_email_accent_color") or "").strip()
    banner_url   = (body.get("birthday_email_banner_url") or "").strip()
    footer_text  = (body.get("birthday_email_footer_text") or "").strip()
    db.execute(text(
        "UPDATE businesses SET birthday_gift_type=:gt, birthday_gift_product=:gp, birthday_email_intro=:ei, "
        "birthday_email_header_color=:hc, birthday_email_accent_color=:ac, "
        "birthday_email_banner_url=:bu, birthday_email_footer_text=:ft WHERE slug=:slug"
    ), {"gt": gift_type, "gp": gift_product, "ei": email_intro,
        "hc": header_color, "ac": accent_color, "bu": banner_url, "ft": footer_text, "slug": slug})
    db.commit()
    return {"status": "updated"}


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
    name        = (body.get("name") or "").strip()
    industry    = (body.get("industry") or "other").strip()
    description = body.get("description")  # None = no change; "" = clear it
    if name:
        db.execute(text("UPDATE businesses SET name=:name WHERE slug=:slug"), {"name": name, "slug": slug})
    db.execute(text("UPDATE businesses SET industry=:ind WHERE slug=:slug"), {"ind": industry, "slug": slug})
    if description is not None:
        db.execute(text("UPDATE businesses SET description=:desc WHERE slug=:slug"), {"desc": description.strip() or None, "slug": slug})
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
        "biz_email":  biz.email,
        "stamps_per_reward": biz.stamps_per_reward,
        "card_title": biz.card_title,
        "biz_plan":   _get_biz_plan(biz),
        "stripe_configured": bool(STRIPE_SECRET_KEY and STRIPE_PRICE_ID_PRO),
        "primary_color": biz.primary_color or "#26170c",
        "accent_color":  biz.accent_color  or "#ffca48",
        "logo_url":      biz.logo_url or "",
    })


@app.get("/biz/{slug}/register", response_class=HTMLResponse)
async def biz_register_page(slug: str, request: Request, ref: str = "", db: Session = Depends(get_db)):
    """Business-specific customer registration page"""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    # Load landing config for this business
    landing_cfg = {}
    programa_cfg = {}
    try:
        row = db.execute(text(
            "SELECT config FROM card_config WHERE business_id=:bid ORDER BY updated_at DESC LIMIT 1"
        ), {"bid": str(biz.id)}).fetchone()
        if not row:
            row = db.execute(text("SELECT config FROM card_config WHERE id=1")).fetchone()
        if row:
            stored = json.loads(row[0])
            landing_cfg = stored.get("landing", {})
            programa_cfg = stored.get("programa", {})
    except Exception:
        pass
    # Fetch first card program for this business to sync visual design
    _reg_prog = None
    try:
        _reg_prog = db.query(models.CardProgram).filter(
            models.CardProgram.business_id == biz.id
        ).first()
    except Exception:
        pass
    # Use card program colors if available, fall back to biz defaults
    _reg_bg      = (_reg_prog.bg_color if _reg_prog and _reg_prog.bg_color else None) or biz.primary_color or "#26170c"
    _reg_accent  = (_reg_prog.accent_color if _reg_prog and _reg_prog.accent_color else None) or biz.accent_color or "#ffca48"
    _reg_text    = (_reg_prog.text_color if _reg_prog and _reg_prog.text_color else None) or "#ffffff"
    _reg_strip   = (_reg_prog.strip_bg_url if _reg_prog else None) or ""
    _reg_spr     = (_reg_prog.stamps_per_reward if _reg_prog and _reg_prog.stamps_per_reward else None) or biz.stamps_per_reward or STAMPS_PER_REWARD
    _reg_reward  = landing_cfg.get("reward_name") or (_reg_prog.reward_name if _reg_prog else None) or "Premio"
    _birthday_enabled = programa_cfg.get("birthday_enabled", True)
    return templates.TemplateResponse("register.html", {
        "request":           request,
        "card_title":        (_reg_prog.name if _reg_prog else None) or biz.card_title or biz.name,
        "biz_name":          biz.name,
        "logo_url":          biz.logo_url or "",
        "primary_color":     _reg_bg,
        "accent_color":      _reg_accent,
        "text_color":        _reg_text,
        "strip_bg_url":      _reg_strip,
        "api_base":          BASE_URL,
        "stamps_per_reward": _reg_spr,
        "reward_name":       _reg_reward,
        "biz_slug":          slug,
        "ref":               ref,
        "birthday_enabled":  _birthday_enabled,
        # Landing customization from config
        "form_title":        landing_cfg.get("form_title", ""),
        "header_text":       landing_cfg.get("header_text", ""),
        "button_text":       landing_cfg.get("button_text", ""),
        "lp_bg_color":       landing_cfg.get("bg_color", ""),
        "lp_text_color":     landing_cfg.get("text_color", ""),
        "lp_button_color":   landing_cfg.get("button_color", ""),
        "lp_link_color":     landing_cfg.get("link_color", ""),
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
    # CardProgram is source of truth for colors; fall back to Business settings
    _biz_card_prog = db.query(models.CardProgram).filter(
        models.CardProgram.business_id == biz.id
    ).first()
    primary_color = (_biz_card_prog.bg_color     if _biz_card_prog and _biz_card_prog.bg_color     else None) or biz.primary_color or "#26170c"
    accent_color  = (_biz_card_prog.accent_color if _biz_card_prog and _biz_card_prog.accent_color else None) or biz.accent_color  or "#ffca48"
    # Generate Google Wallet URL (None if not configured)
    _biz_reward = (_biz_card_prog.reward_name if _biz_card_prog and _biz_card_prog.reward_name else None) or "Premio"
    _biz_logo   = (biz.logo_url if biz.logo_url else "") or ""
    _biz_cname  = (_biz_card_prog.name if _biz_card_prog and _biz_card_prog.name else None) or biz.name
    google_wallet_url = generate_google_wallet_url(
        card_id=card_id,
        biz_slug=slug,
        biz_name=biz.name,
        customer_name=first_name,
        stamps=card.stamps or 0,
        stamps_per_reward=biz.stamps_per_reward or STAMPS_PER_REWARD,
        card_url=card_url,
        primary_color=primary_color,
        accent_color=accent_color,
        reward_name=_biz_reward,
        logo_url=_biz_logo,
        award_balance=card.award_balance or 0,
        card_name=_biz_cname,
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
        "accent_color":       accent_color,
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


@app.get("/api/admin/wallet-devices")
def admin_wallet_devices(pin: str = "", db: Session = Depends(get_db)):
    """Debug: list wallet_devices registrations and attempt a test APNs push."""
    if pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="Acceso denegado")
    rows = db.execute(text(
        "SELECT wd.id, wd.device_library_id, wd.push_token, wd.card_id, wd.pass_type_id, wd.serial_number, "
        "       lc.stamps, c.first_name, c.last_name "
        "FROM wallet_devices wd "
        "LEFT JOIN loyalty_cards lc ON lc.id = wd.card_id "
        "LEFT JOIN customers c ON c.id = lc.customer_id "
        "ORDER BY wd.id DESC LIMIT 50"
    )).fetchall()
    import os as _os
    p12_set  = bool(_os.environ.get("APPLE_P12_B64", ""))
    ptid_set = bool(_os.environ.get("APPLE_PASS_TYPE_ID", ""))
    return {
        "apple_p12_configured": p12_set,
        "apple_pass_type_id_configured": ptid_set,
        "device_count": len(rows),
        "devices": [
            {
                "device_library_id": r[1][:16] + "...",
                "push_token": r[2][:12] + "...",
                "card_id": str(r[3]),
                "serial": r[5],
                "customer": f"{r[7]} {r[8]}",
                "stamps": r[6],
            }
            for r in rows
        ],
    }


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
# RESET OWNER — borra TODOS los datos de un email dado (para pruebas desde 0)
# POST /api/admin/reset-owner?email=zubbigpt@gmail.com&master_pin=XXXX
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/api/admin/reset-owner")
def reset_owner(email: str = "", master_pin: str = "", db: Session = Depends(get_db)):
    """
    Permanently deletes ALL data for a given business email.
    Requires master ADMIN_PIN. Use only for testing / demo resets.
    """
    if master_pin != ADMIN_PIN:
        raise HTTPException(status_code=403, detail="PIN maestro incorrecto")
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    email = email.strip().lower()

    # Fetch the business(es) matching this email
    biz_rows = db.execute(
        text("SELECT id FROM businesses WHERE email=:email"),
        {"email": email}
    ).fetchall()

    if not biz_rows:
        return {"status": "not_found", "email": email}

    deleted = []
    for biz_row in biz_rows:
        bid = str(biz_row[0])
        try:
            # Delete in FK dependency order
            db.execute(text(
                "DELETE FROM push_subscriptions WHERE card_id IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id"
                "  WHERE c.business_id=:bid)"
            ), {"bid": bid})
            db.execute(text(
                "DELETE FROM referrals WHERE referrer_card IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id WHERE c.business_id=:bid)"
                " OR referred_card IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id WHERE c.business_id=:bid)"
            ), {"bid": bid})
            db.execute(text(
                "DELETE FROM stamp_transactions WHERE card_id IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id WHERE c.business_id=:bid)"
            ), {"bid": bid})
            # Clear passcode used_by references before deleting loyalty_cards
            db.execute(text(
                "UPDATE passcodes SET used_by=NULL WHERE used_by IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id WHERE c.business_id=:bid)"
            ), {"bid": bid})
            db.execute(text(
                "UPDATE wallet_devices SET card_id=NULL WHERE card_id IN ("
                "  SELECT lc.id FROM loyalty_cards lc"
                "  JOIN customers c ON lc.customer_id=c.id WHERE c.business_id=:bid)"
            ), {"bid": bid})
            db.execute(text(
                "DELETE FROM loyalty_cards WHERE customer_id IN ("
                "  SELECT id FROM customers WHERE business_id=:bid)"
            ), {"bid": bid})
            db.execute(text("DELETE FROM customers WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM stores WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM passcodes WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM campaigns WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM card_programs WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM custom_qrs WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM card_config WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM scanner_devices WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("DELETE FROM businesses WHERE id=:bid"), {"bid": bid})
            db.commit()
            deleted.append(bid)
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Error al borrar business {bid}: {str(e)}")

    return {
        "status":  "deleted",
        "email":   email,
        "deleted_ids": deleted,
        "message": f"Cuenta eliminada. Puedes registrarte de nuevo con Google en /auth/google"
    }


# ════════════════════════════════════════════════════════════════════════════════
# DELETE BUSINESS ACCOUNT
# ══════════════════════════════════════════════════════════════════════════════
@app.delete("/api/app/businesses/{slug}")
def delete_business(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """
    Permanently delete a business account and ALL its data.
    Requires the business admin PIN as query param: ?pin=XXXX
    Permanently delete a business account and ALL its data.
    """
    # Any business can be deleted by its owner PIN

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
        # loyalty_cards → customers → businesses (no direct business_id on loyalty_cards)
        lc_subq  = "SELECT lc.id FROM loyalty_cards lc JOIN customers c ON c.id=lc.customer_id WHERE c.business_id=:bid"
        cust_subq = "SELECT id FROM customers WHERE business_id=:bid"

        db.execute(text(f"DELETE FROM push_subscriptions WHERE card_id IN ({lc_subq})"), {"bid": bid})
        db.execute(text(f"DELETE FROM referrals WHERE referrer_card IN ({lc_subq}) OR referred_card IN ({lc_subq})"), {"bid": bid})
        db.execute(text(f"DELETE FROM stamp_transactions WHERE card_id IN ({lc_subq})"), {"bid": bid})
        # Clear FK references before deleting loyalty_cards
        db.execute(text(f"UPDATE passcodes SET used_by=NULL WHERE used_by IN ({lc_subq})"), {"bid": bid})
        db.execute(text(f"UPDATE wallet_devices SET card_id=NULL WHERE card_id IN ({lc_subq})"), {"bid": bid})
        db.execute(text(f"DELETE FROM wallet_devices WHERE card_id IN ({lc_subq})"), {"bid": bid})
        db.execute(text(f"DELETE FROM loyalty_cards WHERE customer_id IN ({cust_subq})"), {"bid": bid})
        db.execute(text("DELETE FROM customers WHERE business_id=:bid"), {"bid": bid})
        # Business-level tables
        db.execute(text("DELETE FROM scanner_devices WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM stores WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM passcodes WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM campaigns WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM card_programs WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM custom_qrs WHERE business_id=:bid"), {"bid": bid})
        db.execute(text("DELETE FROM card_config WHERE business_id=:bid"), {"bid": bid})
        # activity_log may not exist — wrap in savepoint so failure doesn't rollback the whole tx
        try:
            db.execute(text("SAVEPOINT sp_activity"))
            db.execute(text("DELETE FROM activity_log WHERE business_id=:bid"), {"bid": bid})
            db.execute(text("RELEASE SAVEPOINT sp_activity"))
        except Exception:
            db.execute(text("ROLLBACK TO SAVEPOINT sp_activity"))
        # Finally delete the business itself
        db.execute(text("DELETE FROM businesses WHERE id=:bid"), {"bid": bid})
        db.commit()
        return {"status": "deleted", "slug": slug}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error al eliminar: {str(e)}")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_rich(request: Request):
    return templates.TemplateResponse("dashboard_admin.html", {
        "request": request,
        "biz_name": "",
        "biz_slug": "",
        "biz_api_base": BASE_URL,
        "biz_api_key": "",
        "biz_email": "",
        "primary_color": "#3A3426",
        "accent_color": "#F5E6C8",
        "logo_url": "",
    })


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
        "SELECT id, name, pin, notes, active, created_at, latitude, longitude, geo_radius_m, geo_push_msg, address FROM stores WHERE business_id=:bid ORDER BY created_at ASC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"stores": [
        {"id": str(r[0]), "name": r[1], "pin": r[2], "notes": r[3] or "", "active": bool(r[4]), "created_at": str(r[5]),
         "latitude": r[6], "longitude": r[7], "geo_radius_m": r[8] or 300, "geo_push_msg": r[9] or "", "address": r[10] or ""}
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
        "INSERT INTO stores (id, business_id, name, pin, notes, active, latitude, longitude, geo_radius_m, geo_push_msg, address) "
        "VALUES (:id, :bid, :name, :pin, :notes, :active, :lat, :lng, :radius, :geo_msg, :address)"
    ), {"id": store_id, "bid": str(biz.id), "name": payload.get("name", ""),
        "pin": str(payload.get("pin", "")), "notes": payload.get("notes", ""),
        "active": payload.get("active", True),
        "lat": payload.get("latitude"), "lng": payload.get("longitude"),
        "radius": payload.get("geo_radius_m", 300), "geo_msg": payload.get("geo_push_msg", ""),
        "address": payload.get("address", "")})
    db.commit()
    return {"id": store_id, "status": "created"}


@app.put("/api/biz/{slug}/stores/{store_id}")
def update_store(slug: str, store_id: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text(
        "UPDATE stores SET name=:name, pin=:pin, notes=:notes, active=:active, "
        "latitude=:lat, longitude=:lng, geo_radius_m=:radius, geo_push_msg=:geo_msg, address=:address "
        "WHERE id=:id AND business_id=:bid"
    ), {"name": payload.get("name", ""), "pin": str(payload.get("pin", "")),
        "notes": payload.get("notes", ""), "active": payload.get("active", True),
        "lat": payload.get("latitude"), "lng": payload.get("longitude"),
        "radius": payload.get("geo_radius_m", 300), "geo_msg": payload.get("geo_push_msg", ""),
        "address": payload.get("address", ""),
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
# SCANNER DEVICE AUTHORIZATION
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/app/scanner-login", response_class=HTMLResponse)
async def scanner_login_page(request: Request):
    """Generic fallback — redirects to /biz/{slug}/scanner-login if slug saved in cookie, else shows help."""
    return templates.TemplateResponse("scanner_login.html", {"request": request, "base_url": BASE_URL})


@app.get("/biz/{slug}/scanner-login", response_class=HTMLResponse)
async def scanner_login_biz_page(request: Request, slug: str, db: Session = Depends(get_db)):
    """Business-specific scanner login — employee only needs their PIN, no slug required."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return templates.TemplateResponse("scanner_login_biz.html", {
        "request":       request,
        "base_url":      BASE_URL,
        "slug":          slug,
        "business_name": biz.name,
        "primary_color": biz.primary_color or "#3A3426",
        "accent_color":  biz.accent_color  or "#FFF3CF",
    })


@app.post("/api/scanner/auth")
async def scanner_auth(request: Request, db: Session = Depends(get_db)):
    """
    Employee login from scanner device.
    Body: {slug, pin, device_token, device_name}
    Returns:
      - {status: 'pending', device_token} if new/pending device
      - {status: 'approved', slug, store_name, store_id, session_token} if approved
      - {status: 'rejected'} if revoked
      - 403 if bad credentials
    """
    body = await request.json()
    slug         = (body.get("slug") or "").strip().lower()
    pin          = str(body.get("pin") or "").strip()
    device_token = (body.get("device_token") or "").strip()
    device_name  = (body.get("device_name") or "Este dispositivo").strip()

    if not slug or not pin or not device_token:
        raise HTTPException(status_code=400, detail="Faltan campos requeridos")

    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    # Validate credentials: accept store PIN only (not admin)
    store_row = db.execute(text(
        "SELECT id, name FROM stores WHERE business_id=:bid AND pin=:pin AND active=TRUE LIMIT 1"
    ), {"bid": str(biz.id), "pin": pin}).fetchone()

    if not store_row:
        raise HTTPException(status_code=403, detail="PIN incorrecto o tienda inactiva")

    store_id   = str(store_row[0])
    store_name = store_row[1]

    # Check existing device record
    dev = db.execute(text(
        "SELECT id, status FROM scanner_devices WHERE device_token=:tok AND business_id=:bid LIMIT 1"
    ), {"tok": device_token, "bid": str(biz.id)}).fetchone()

    if dev:
        status = dev[1]
        if status == "approved":
            # Update last_seen and store association (in case PIN changed stores)
            db.execute(text(
                "UPDATE scanner_devices SET last_seen_at=NOW(), store_id=:sid, store_name=:sname "
                "WHERE device_token=:tok AND business_id=:bid"
            ), {"sid": store_id, "sname": store_name, "tok": device_token, "bid": str(biz.id)})
            db.commit()
            return {"status": "approved", "slug": slug, "store_name": store_name,
                    "store_id": store_id, "business_name": biz.name}
        elif status == "rejected":
            return {"status": "rejected"}
        else:
            return {"status": "pending", "device_token": device_token}
    else:
        # Register new device as pending
        db.execute(text(
            "INSERT INTO scanner_devices (business_id, store_id, device_token, device_name, store_name, status) "
            "VALUES (:bid, :sid, :tok, :dname, :sname, 'pending')"
        ), {"bid": str(biz.id), "sid": store_id, "tok": device_token,
            "dname": device_name, "sname": store_name})
        db.commit()
        return {"status": "pending", "device_token": device_token}


@app.get("/api/scanner/device-status/{device_token}")
def scanner_device_status(device_token: str, db: Session = Depends(get_db)):
    """Polling endpoint — employee page polls this to detect when admin approves the device."""
    dev = db.execute(text(
        "SELECT status, store_name, store_id, b.slug, b.name "
        "FROM scanner_devices sd "
        "JOIN businesses b ON b.id = sd.business_id "
        "WHERE sd.device_token=:tok LIMIT 1"
    ), {"tok": device_token}).fetchone()

    if not dev:
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")

    if dev[0] == "approved":
        return {"status": "approved", "store_name": dev[1], "store_id": str(dev[2]) if dev[2] else "",
                "slug": dev[3], "business_name": dev[4]}
    return {"status": dev[0]}


@app.get("/api/biz/{slug}/scanner-devices")
def list_scanner_devices(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Admin: list all registered scanner devices for this business."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    rows = db.execute(text(
        "SELECT sd.id, sd.device_token, sd.device_name, sd.store_name, sd.status, "
        "sd.created_at, sd.approved_at, sd.last_seen_at "
        "FROM scanner_devices sd WHERE sd.business_id=:bid ORDER BY sd.created_at DESC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"devices": [
        {"id": str(r[0]), "device_token": r[1], "device_name": r[2] or "Sin nombre",
         "store_name": r[3] or "", "status": r[4],
         "created_at": r[5].isoformat() if r[5] else None,
         "approved_at": r[6].isoformat() if r[6] else None,
         "last_seen_at": r[7].isoformat() if r[7] else None}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/scanner-devices/{device_id}/approve")
def approve_scanner_device(slug: str, device_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Admin: approve a pending scanner device."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text(
        "UPDATE scanner_devices SET status='approved', approved_at=NOW() "
        "WHERE id=:id AND business_id=:bid"
    ), {"id": device_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "approved"}


@app.post("/api/biz/{slug}/scanner-devices/{device_id}/revoke")
def revoke_scanner_device(slug: str, device_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Admin: revoke an approved scanner device."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(text(
        "UPDATE scanner_devices SET status='rejected' WHERE id=:id AND business_id=:bid"
    ), {"id": device_id, "bid": str(biz.id)})
    db.commit()
    return {"status": "revoked"}


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
async def redeem_passcode(slug: str, payload: dict = Body(...), db: Session = Depends(get_db)):
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
        "UPDATE loyalty_cards SET stamps = stamps + :s, total_stamps = COALESCE(total_stamps, 0) + :s WHERE id=:cid"
    ), {"s": stamps, "cid": card_id})
    db.execute(text(
        "INSERT INTO stamp_transactions (id, card_id, stamps_added, transaction_type, note, created_at) "
        "VALUES (:id, :cid, :s, 'passcode', :note, NOW())"
    ), {"id": str(uuid.uuid4()), "cid": card_id, "s": stamps, "note": f"PassCode: {code}"})
    db.commit()
    # Push live Wallet update after passcode stamp
    try:
        await _push_wallet_update(card_id, db)
    except Exception:
        pass
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
        "SELECT id, name, subject, type, status, segment, created_at, sent_at, scheduled_at "
        "FROM campaigns WHERE business_id=:bid ORDER BY created_at DESC"
    ), {"bid": str(biz.id)}).fetchall()
    return {"campaigns": [
        {"id": str(r[0]), "name": r[1], "subject": r[2] or "", "type": r[3] or "email",
         "status": r[4] or "draft", "segment": r[5] or "all",
         "created_at": str(r[6]), "sent_at": str(r[7]) if r[7] else None,
         "scheduled_at": str(r[8]) if r[8] else None}
        for r in rows
    ]}


@app.post("/api/biz/{slug}/campaigns")
def create_campaign(slug: str, pin: str = "", payload: dict = Body(...), db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    _require_pro(biz)   # ← Free plan cannot create campaigns
    camp_id = str(uuid.uuid4())
    scheduled_at = payload.get("scheduled_at")  # ISO string or None
    db.execute(text(
        "INSERT INTO campaigns (id, business_id, name, subject, body, type, status, segment, scheduled_at) "
        "VALUES (:id, :bid, :name, :subject, :body, :type, :status, :segment, :scheduled_at)"
    ), {"id": camp_id, "bid": str(biz.id), "name": payload.get("name", ""),
        "subject": payload.get("subject", ""), "body": payload.get("body", ""),
        "type": payload.get("type", "email"), "status": payload.get("status", "draft"),
        "segment": payload.get("segment", "all"), "scheduled_at": scheduled_at})
    db.commit()
    return {"id": camp_id, "status": "created", "scheduled_at": scheduled_at}


@app.post("/api/biz/{slug}/campaigns/{campaign_id}/send")
async def send_campaign(slug: str, campaign_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Send email or push campaign to segmented customers"""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    _require_pro(biz)   # ← Free plan cannot send campaigns
    camp = db.execute(text(
        "SELECT name, subject, body, segment, type FROM campaigns WHERE id=:id AND business_id=:bid"
    ), {"id": campaign_id, "bid": str(biz.id)}).fetchone()
    if not camp:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")
    camp_name, subject, body_txt, segment, camp_type = camp
    segment = segment or "all"
    camp_type = camp_type or "email"

    if camp_type == "push":
        title_txt = subject or camp_name or "ZubCard"
        msg_txt   = body_txt or ""
        # Apple Wallet (APNs) — iOS
        apns_result = await _send_apns_campaign(db, str(biz.id), title_txt, msg_txt, segment)
        # Android / browser web push
        web_sent = web_failed = 0
        q_web = (
            "SELECT ps.endpoint, ps.p256dh, ps.auth FROM push_subscriptions ps "
            "JOIN loyalty_cards lc ON lc.id=ps.card_id "
            "JOIN customers c ON c.id=lc.customer_id WHERE c.business_id=:bid"
        )
        if segment == "active":
            q_web += " AND lc.stamps > 0"
        web_rows = db.execute(text(q_web), {"bid": str(biz.id)}).fetchall()
        if web_rows and VAPID_PUBLIC and VAPID_PRIVATE:
            web_result = _send_push_to_subscriptions(web_rows, title_txt, msg_txt)
            web_sent   = web_result["sent"]
            web_failed = web_result["failed"]
        db.execute(text("UPDATE campaigns SET status='sent', sent_at=NOW() WHERE id=:id"), {"id": campaign_id})
        db.commit()
        total_sent = apns_result["sent"] + web_sent
        return {"sent": total_sent, "apple_wallet": apns_result["sent"],
                "android_web": web_sent, "status": "sent", "type": "push"}

    # ── EMAIL ──
    q_base = (
        "SELECT c.email, c.first_name FROM customers c "
        "JOIN loyalty_cards lc ON lc.customer_id=c.id "
        "WHERE c.business_id=:bid AND c.opt_in_email=TRUE AND c.email NOT LIKE '%placeholder%'"
    )
    if segment == "active":
        q_base += " AND lc.stamps > 0"
    customers = db.execute(text(q_base), {"bid": str(biz.id)}).fetchall()
    sent = 0
    for cust in customers:
        try:
            body_html = f"<p>{(body_txt or '').replace('{nombre}', cust[1] or '')}</p>"
            subject_text = (subject or "").replace("{nombre}", cust[1] or "")
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
# STRIPE / SUSCRIPCIONES
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/subscription")
def get_subscription(slug: str, pin: str = "", db: Session = Depends(get_db)):
    """Return plan + Stripe subscription info for this business."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    plan = _get_biz_plan(biz)
    stripe_configured = bool(STRIPE_SECRET_KEY and STRIPE_PRICE_ID_PRO)
    sub_status = getattr(biz, "stripe_subscription_status", None) or (
        "active" if plan == "pro" else "inactive"
    )
    period_end = getattr(biz, "stripe_current_period_end", None)
    return {
        "plan": plan,
        "stripe_configured": stripe_configured,
        "stripe_publishable_key": STRIPE_PUBLISHABLE_KEY if stripe_configured else None,
        "subscription_status": sub_status,
        "current_period_end": period_end.isoformat() if period_end else None,
        "price_display": STRIPE_PRO_PRICE_DISPLAY,
    }


@app.post("/api/biz/{slug}/subscription/checkout")
async def create_checkout_session(slug: str, request: Request, db: Session = Depends(get_db)):
    """Create a Stripe Checkout Session for the Pro plan."""
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID_PRO:
        raise HTTPException(status_code=503,
            detail="Stripe no configurado. Añade STRIPE_SECRET_KEY y STRIPE_PRICE_ID_PRO en Railway.")
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    if _get_biz_plan(biz) == "pro":
        raise HTTPException(status_code=400, detail="Ya tienes el plan Pro activo")

    # Get or create Stripe customer
    customer_id = getattr(biz, "stripe_customer_id", None)
    if not customer_id:
        customer = _stripe_sdk.Customer.create(
            email=biz.email,
            name=biz.name,
            metadata={"biz_id": str(biz.id), "biz_slug": slug},
        )
        customer_id = customer.id
        db.execute(text("UPDATE businesses SET stripe_customer_id=:cid WHERE id=:bid"),
                   {"cid": customer_id, "bid": str(biz.id)})
        db.commit()

    try:
        session = _stripe_sdk.checkout.Session.create(
            customer=customer_id,
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID_PRO, "quantity": 1}],
            success_url=f"{BASE_URL}/biz/{slug}/dashboard?stripe_success=1",
            cancel_url=f"{BASE_URL}/biz/{slug}/dashboard?stripe_cancel=1",
            subscription_data={"metadata": {"biz_id": str(biz.id), "biz_slug": slug}, "trial_period_days": 14},
            allow_promotion_codes=True,
            customer_update={"name": "auto", "address": "auto"},   # required when tax_id_collection is enabled for existing customers
            tax_id_collection={"enabled": True},
            billing_address_collection="required",
            locale="es",
        )
    except Exception as stripe_err:
        raise HTTPException(status_code=400, detail=str(stripe_err))
    return {"checkout_url": session.url, "session_id": session.id}


@app.post("/api/biz/{slug}/subscription/portal")
async def create_billing_portal(slug: str, request: Request, db: Session = Depends(get_db)):
    """Create a Stripe Customer Portal session for managing billing."""
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe no configurado")
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    customer_id = getattr(biz, "stripe_customer_id", None)
    if not customer_id:
        raise HTTPException(status_code=400,
            detail="No hay suscripción activa. Suscríbete primero al plan Pro.")

    portal = _stripe_sdk.billing_portal.Session.create(
        customer=customer_id,
        return_url=f"{BASE_URL}/biz/{slug}/dashboard",
    )
    return {"portal_url": portal.url}


@app.post("/api/admin/stripe-sync/{slug}")
async def admin_stripe_sync(slug: str, request: Request, db: Session = Depends(get_db)):
    """Admin: forzar sync del plan desde Stripe o set manual con customer_id/sub_id."""
    body = await request.json()
    verify_pin(str(body.get("pin", "")), db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    from datetime import datetime as _dt

    # Si se pasan customer_id y sub_id directo, no llamamos Stripe API
    customer_id = body.get("customer_id") or getattr(biz, "stripe_customer_id", None)
    sub_id      = body.get("subscription_id")
    period_end  = body.get("period_end")  # ISO string opcional

    if not customer_id:
        raise HTTPException(status_code=400, detail="Sin stripe_customer_id")

    if not sub_id:
        # Buscar en Stripe
        try:
            result = _stripe_sdk.Subscription.list(customer=customer_id, limit=1)
            active = [s for s in result.data if s.get("status") in ("active", "trialing")]
            if not active:
                raise HTTPException(status_code=404, detail="No hay suscripción activa en Stripe")
            sub = active[0]
            sub_id = sub["id"]
            pe = _dt.fromtimestamp(sub["current_period_end"])
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error Stripe: {str(e)}")
    else:
        pe = _dt.fromisoformat(period_end) if period_end else _dt(2026, 4, 22)

    db.execute(text(
        "UPDATE businesses SET plan='pro', stripe_customer_id=:cid, stripe_subscription_id=:sid, "
        "stripe_subscription_status='active', stripe_current_period_end=:pe WHERE id=:bid"
    ), {"cid": customer_id, "sid": sub_id, "pe": pe, "bid": str(biz.id)})
    db.commit()
    return {"ok": True, "plan": "pro", "customer_id": customer_id, "subscription_id": sub_id, "period_end": pe.isoformat()}


@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """Handle Stripe webhook events (subscription lifecycle)."""
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    from datetime import datetime as _dt
    try:
        event = _stripe_sdk.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except _stripe_sdk.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    etype = event["type"]
    data  = event["data"]["object"]

    def _biz_by_customer(cid):
        return db.execute(text(
            "SELECT id FROM businesses WHERE stripe_customer_id=:cid"
        ), {"cid": cid}).fetchone()

    try:
        if etype == "checkout.session.completed":
            # Marca pro y confirma email (por si registró y pagó en el mismo flow)
            cid = data.get("customer")
            sid = data.get("subscription")
            # Try by stripe_customer_id first, fallback to metadata biz_id/biz_slug
            biz_row = _biz_by_customer(cid)
            if not biz_row:
                meta = data.get("metadata") or {}
                biz_slug = (data.get("subscription_data") or {}).get("metadata", {}).get("biz_slug") or meta.get("biz_slug")
                biz_id   = (data.get("subscription_data") or {}).get("metadata", {}).get("biz_id") or meta.get("biz_id")
                if biz_id:
                    biz_row = db.execute(text("SELECT id FROM businesses WHERE id=:bid"), {"bid": biz_id}).fetchone()
                elif biz_slug:
                    biz_row = db.execute(text("SELECT id FROM businesses WHERE slug=:s"), {"s": biz_slug}).fetchone()
            if biz_row and sid:
                db.execute(text(
                    "UPDATE businesses SET plan='pro', stripe_customer_id=:cid, stripe_subscription_id=:sid, "
                    "stripe_subscription_status='active', email_confirmed=TRUE, "
                    "email_confirm_token=NULL WHERE id=:bid"
                ), {"cid": cid, "sid": sid, "bid": str(biz_row[0])})
                db.commit()
                print(f"✅ checkout.session.completed — biz {biz_row[0]} → pro + email confirmed")

        elif etype in ("customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"):
            cid    = data.get("customer")
            sid    = data.get("id")
            status = data.get("status", "")
            pe_ts  = data.get("current_period_end")
            pe     = _dt.fromtimestamp(pe_ts) if pe_ts else None
            biz_row = _biz_by_customer(cid)
            if biz_row:
                # Proteger cuentas con plan bloqueado (owner-account, etc.)
                stored = db.execute(text(
                    "SELECT stripe_subscription_id FROM businesses WHERE id=:bid"
                ), {"bid": str(biz_row[0])}).scalar()
                if stored and not stored.startswith("sub_"):
                    print(f"⏭️ {etype} ignorado — plan bloqueado ({stored})")
                else:
                    new_plan = "pro" if status in ("active", "trialing") else "free"
                    db.execute(text(
                        "UPDATE businesses SET plan=:plan, stripe_subscription_id=:sid, "
                        "stripe_subscription_status=:status, stripe_current_period_end=:pe "
                        "WHERE id=:bid"
                    ), {"plan": new_plan, "sid": sid, "status": status, "pe": pe, "bid": str(biz_row[0])})
                    db.commit()
                    print(f"✅ {etype} — biz {biz_row[0]} status={status} plan={new_plan}")

        elif etype == "invoice.payment_failed":
            cid = data.get("customer")
            biz_row = _biz_by_customer(cid)
            if biz_row:
                db.execute(text(
                    "UPDATE businesses SET stripe_subscription_status='past_due' WHERE id=:bid"
                ), {"bid": str(biz_row[0])})
                db.commit()
                print(f"⚠️ invoice.payment_failed — biz {biz_row[0]}")

        else:
            print(f"⏭️ Stripe event ignorado: {etype}")

    except Exception as e:
        import traceback
        print(f"❌ Stripe webhook error [{etype}]: {e}")
        traceback.print_exc()
        # Siempre 200 para que Stripe no reintente — error loggeado en Railway

    return {"received": True}


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
        "SELECT id, name, emoji, stamps_per_reward, reward_name, bg_color, accent_color, text_color, status, sort_order, strip_bg_url, created_at "
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
    # Free plan: max 1 card program
    if _get_biz_plan(biz) != "pro":
        count = db.execute(text(
            "SELECT COUNT(*) FROM card_programs WHERE business_id=:bid AND status='active'"
        ), {"bid": str(biz.id)}).scalar() or 0
        if count >= FREE_LIMITS["max_card_programs"]:
            raise HTTPException(status_code=402, detail="upgrade_required")
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

@app.patch("/api/biz/{slug}/card-programs/{program_id}")
async def update_card_program(slug: str, program_id: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    body = await request.json()
    db.execute(text("""
        UPDATE card_programs SET
            name             = :name,
            emoji            = :emoji,
            stamps_per_reward= :stamps,
            reward_name      = :reward,
            bg_color         = :bg,
            accent_color     = :accent,
            text_color       = :text_color
        WHERE id=:id AND business_id=:bid
    """), {
        "name":   body.get("name", ""),
        "emoji":  body.get("emoji", "🎁"),
        "stamps": int(body.get("stamps_per_reward", 10)),
        "reward": body.get("reward_name", "Premio"),
        "bg":     body.get("bg_color", "#0a0a0a"),
        "accent": body.get("accent_color", "#00e676"),
        "text_color": body.get("text_color", "#ffffff"),
        "id":     program_id,
        "bid":    str(biz.id),
    })
    db.commit()
    return {"status": "updated"}


@app.get("/biz/{slug}/logo.png")
def serve_biz_logo(slug: str, db: Session = Depends(get_db)):
    """Serve business logo as PNG — for use in emails where data: URIs are blocked."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404)
    logo = getattr(biz, "logo_url", "") or ""
    if not logo or not logo.startswith("data:image"):
        raise HTTPException(status_code=404)
    import base64 as _b64, io as _io
    _, b64data = logo.split(",", 1)
    raw = _b64.b64decode(b64data)
    return StreamingResponse(_io.BytesIO(raw), media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"})


@app.get("/biz/{slug}/birthday-banner.jpg")
def serve_birthday_banner(slug: str, db: Session = Depends(get_db)):
    """Serve birthday email banner image as JPEG — needed because Gmail blocks data: URIs."""
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404)
    banner = getattr(biz, "birthday_email_banner_url", "") or ""
    if not banner or not banner.startswith("data:image"):
        raise HTTPException(status_code=404)
    import base64 as _b64, io as _io
    # Strip the data URL prefix
    _, b64data = banner.split(",", 1)
    raw = _b64.b64decode(b64data)
    return StreamingResponse(_io.BytesIO(raw), media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"})


@app.post("/api/biz/{slug}/birthday-email-banner")
async def upload_birthday_banner(
    slug: str,
    pin: str = "",
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload banner image for birthday email header. Stores as base64 data URL."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    ct = file.content_type or ""
    if not ct.startswith("image/"):
        raise HTTPException(status_code=400, detail="Solo se permiten imágenes (JPG, PNG, WEBP)")
    raw = await file.read()
    if len(raw) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Imagen demasiado grande (máx 10 MB)")
    try:
        from PIL import Image as _PILImg
        import io as _io
        import base64 as _b64
        img = _PILImg.open(_io.BytesIO(raw)).convert("RGB")
        # Resize to 1200×480 (email header @2x) — cover crop
        TW, TH = 1200, 480
        iw, ih = img.size
        scale_f = max(TW / iw, TH / ih)
        nw, nh = round(iw * scale_f), round(ih * scale_f)
        img = img.resize((nw, nh), _PILImg.LANCZOS)
        left = (nw - TW) // 2
        top  = (nh - TH) // 2
        img  = img.crop((left, top, left + TW, top + TH))
        buf = _io.BytesIO()
        img.save(buf, format="JPEG", quality=85, optimize=True)
        data_url = "data:image/jpeg;base64," + _b64.b64encode(buf.getvalue()).decode()
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Error procesando imagen: {e}")
    db.execute(
        text("UPDATE businesses SET birthday_email_banner_url=:url WHERE slug=:slug"),
        {"url": data_url, "slug": slug}
    )
    db.commit()
    return {"status": "ok", "banner_url": data_url}


@app.post("/api/biz/{slug}/card-programs/{program_id}/strip-bg")
async def upload_strip_bg(
    slug: str, program_id: str,
    pin: str = "",
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload a background image for the stamp strip.
    Accepts JPG/PNG/WEBP, resizes to 750×246 (2×), stores as base64 data URL in DB."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    # Validate content type
    ct = file.content_type or ""
    if not ct.startswith("image/"):
        raise HTTPException(status_code=400, detail="Solo se permiten imágenes (JPG, PNG, WEBP)")

    raw = await file.read()
    if len(raw) > 8 * 1024 * 1024:   # 8 MB limit
        raise HTTPException(status_code=413, detail="Imagen demasiado grande (máx 8 MB)")

    try:
        from PIL import Image as _PILImg
        import io as _io
        import base64 as _b64
        img = _PILImg.open(_io.BytesIO(raw)).convert("RGB")
        # Resize/crop to 750×246 (strip @2x) maintaining aspect ratio (cover)
        TW, TH = 750, 246
        iw, ih = img.size
        scale_f = max(TW / iw, TH / ih)
        nw, nh = round(iw * scale_f), round(ih * scale_f)
        img = img.resize((nw, nh), _PILImg.LANCZOS)
        left = (nw - TW) // 2
        top  = (nh - TH) // 2
        img  = img.crop((left, top, left + TW, top + TH))
        buf = _io.BytesIO()
        img.save(buf, format="JPEG", quality=82, optimize=True)
        data_url = "data:image/jpeg;base64," + _b64.b64encode(buf.getvalue()).decode()
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Error procesando imagen: {e}")

    db.execute(
        text("UPDATE card_programs SET strip_bg_url=:url WHERE id=:id AND business_id=:bid"),
        {"url": data_url, "id": program_id, "bid": str(biz.id)}
    )
    db.commit()
    # Return just the data URL (truncated for the response)
    return {"status": "ok", "strip_bg_url": data_url}


@app.delete("/api/biz/{slug}/card-programs/{program_id}/strip-bg")
def delete_strip_bg(slug: str, program_id: str, pin: str = "", db: Session = Depends(get_db)):
    """Remove the strip background image from a card program."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    db.execute(
        text("UPDATE card_programs SET strip_bg_url=NULL WHERE id=:id AND business_id=:bid"),
        {"id": program_id, "bid": str(biz.id)}
    )
    db.commit()
    return {"status": "deleted"}


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
# EMAIL SETTINGS PER-BUSINESS
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/biz/{slug}/email-settings")
def get_email_settings(slug: str, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return {
        "email_from_name": biz.email_from_name or "",
        "email_reply_to":  biz.email_reply_to  or "",
        "email_smtp_host": biz.email_smtp_host  or "",
        "email_smtp_port": biz.email_smtp_port  or 587,
        "email_smtp_user": biz.email_smtp_user  or "",
        "email_smtp_pass": "••••••••" if biz.email_smtp_pass else "",  # never expose
        "has_custom_smtp": bool(biz.email_smtp_host and biz.email_smtp_user and biz.email_smtp_pass),
    }


@app.post("/api/biz/{slug}/email-settings")
async def save_email_settings(slug: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    body = await request.json()

    biz.email_from_name = body.get("email_from_name", biz.email_from_name or "").strip() or None
    biz.email_reply_to  = body.get("email_reply_to",  biz.email_reply_to  or "").strip() or None
    biz.email_smtp_host = body.get("email_smtp_host", biz.email_smtp_host or "").strip() or None
    smtp_port = body.get("email_smtp_port")
    biz.email_smtp_port = int(smtp_port) if smtp_port else None
    biz.email_smtp_user = body.get("email_smtp_user", biz.email_smtp_user or "").strip() or None
    # Only update password if a real value is sent (not the placeholder dots)
    new_pass = body.get("email_smtp_pass", "").strip()
    if new_pass and not new_pass.startswith("•"):
        biz.email_smtp_pass = new_pass

    db.commit()
    db.refresh(biz)
    return {"status": "ok", "email_from_name": biz.email_from_name, "email_reply_to": biz.email_reply_to}


@app.post("/api/biz/{slug}/birthday-voucher/send-test")
async def send_birthday_voucher_test(slug: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    """Send a test birthday voucher email to any given email address (no date/DB restrictions)."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    body = await request.json()
    to_email     = (body.get("to_email") or "").strip()
    name         = (body.get("name") or "Cliente").strip()
    discount_pct = int(body.get("discount_pct", 20))
    if not to_email:
        raise HTTPException(status_code=400, detail="to_email es obligatorio")

    gift_type    = getattr(biz, "birthday_gift_type", "discount") or "discount"
    gift_product = getattr(biz, "birthday_gift_product", "") or ""
    email_intro  = getattr(biz, "birthday_email_intro", "") or ""
    hdr_color    = getattr(biz, "birthday_email_header_color", "") or getattr(biz, "primary_color", "") or "#1a1a1a"
    acc_color    = getattr(biz, "birthday_email_accent_color", "") or getattr(biz, "accent_color", "") or "#c8a96e"
    banner_url   = getattr(biz, "birthday_email_banner_url", "") or ""
    footer_text  = getattr(biz, "birthday_email_footer_text", "") or ""
    logo_url     = getattr(biz, "logo_url", "") or ""

    qr_url = f"{BASE_URL}/biz/{slug}/birthday/test-preview/qr.png"
    # Serve banner as real URL — Gmail blocks data: URIs
    served_banner = f"{BASE_URL}/biz/{slug}/birthday-banner.jpg" if banner_url else ""

    html = _build_birthday_email_html(
        name=name, biz_name=biz.name, qr_url=qr_url,
        gift_type=gift_type, gift_product=gift_product, discount_pct=discount_pct,
        hdr_color=hdr_color, acc_color=acc_color, banner_url=served_banner,
        logo_url=logo_url, email_intro=email_intro, footer_text=footer_text,
        is_test=True, slug=slug
    )

    sent = send_email(to_email=to_email, subject=f"[TEST] ¡Feliz Cumpleaños, {name}! Tu regalo de {biz.name} te espera", html_body=html)
    if sent:
        return {"status": "sent", "to": to_email}
    raise HTTPException(status_code=503, detail="No se pudo enviar el email de prueba")


@app.post("/api/biz/{slug}/email-settings/test")
async def test_email_settings(slug: str, request: Request, pin: str = "", db: Session = Depends(get_db)):
    """Send a test email using the business email config to verify it works."""
    verify_pin(pin, db)
    biz = get_business_by_slug(slug, db)
    if not biz:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    body = await request.json()
    to_email = body.get("to_email", biz.email).strip()

    html = f"""
    <div style='font-family:sans-serif;max-width:400px;margin:40px auto;padding:24px;background:#fff8f5;border-radius:12px'>
      <h2 style='color:#26170c'>✅ Email configurado correctamente</h2>
      <p style='color:#555'>Este es un email de prueba enviado desde <strong>{biz.name}</strong>.</p>
      <p style='color:#555'>Tu configuración de email funciona. Los clientes recibirán sus correos de bienvenida desde este remitente.</p>
      <hr style='border:none;border-top:1px solid #eee;margin:16px 0'>
      <p style='color:#aaa;font-size:12px'>Powered by ZubCard</p>
    </div>
    """
    sent = send_email(
        to_email  = to_email,
        subject   = f"✅ Test de email — {biz.name}",
        html_body = html,
        from_name = biz.email_from_name or biz.name or "",
        reply_to  = biz.email_reply_to  or "",
        smtp_host = biz.email_smtp_host or "",
        smtp_port = biz.email_smtp_port or 0,
        smtp_user = biz.email_smtp_user or "",
        smtp_pass = biz.email_smtp_pass or "",
    )
    if sent:
        return {"status": "sent", "to": to_email}
    else:
        raise HTTPException(status_code=503, detail="No se pudo enviar el email. Revisa la configuración SMTP.")


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
               st.transaction_type, st.stamps_added, st.note, st.store,
               COALESCE(st.source, 'admin') as source,
               COALESCE(st.store, '') as store_name
        FROM stamp_transactions st
        JOIN loyalty_cards lc ON lc.id = st.card_id
        JOIN customers c ON c.id = lc.customer_id
        WHERE c.business_id = :bid
          AND c.email NOT LIKE '%placeholder%'
          {type_filter}
        ORDER BY st.created_at DESC
        LIMIT :limit OFFSET :offset
    """), {"bid": bid, "limit": limit, "offset": offset}).fetchall()

    def source_label(source: str, store: str) -> str:
        if source == "scanner" and store:
            return f"📱 {store}"
        elif source == "scanner":
            return "📱 Scanner"
        elif source == "passcode":
            return "🔑 Passcode"
        elif source == "register":
            return "🆕 Registro"
        else:
            return "💻 Admin"

    return {
        "stats": stats,
        "rows": [
            {
                "created_at": r[0].isoformat() if r[0] else None,
                "name": f"{r[1] or ''} {r[2] or ''}".strip() or "—",
                "email": r[3] or "",
                "type": r[4] or "stamp",
                "amount": int(r[5] or 0),
                "note": r[6] or "",
                "source": r[8] or "admin",
                "source_label": source_label(r[8] or "admin", r[9] or ""),
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
            # Link google_id to existing account and mark confirmed
            biz.google_id = google_sub
            biz.email_confirmed = True
            db.commit()

    if not biz:
        # Create new business account via Google
        base_slug = generate_slug(full_name)
        slug = base_slug
        counter = 1
        while db.query(models.Business).filter(models.Business.slug == slug).first():
            slug = f"{base_slug}{counter}"
            counter += 1

        random_pin = "".join(secrets.choice(string.digits) for _ in range(6))
        biz = models.Business(
            name            = full_name,
            slug            = slug,
            email           = email,
            google_id       = google_sub,
            admin_pin       = random_pin,
            email_confirmed = True,   # Google verifies email — no confirmation needed
            api_key         = generate_api_key(),
            industry        = "other",
            plan            = "free",
        )
        db.add(biz)
        db.commit()
        db.refresh(biz)

        # Create default card_config (safe INSERT — no UNIQUE constraint assumed)
        db.execute(text(
            "INSERT INTO card_config (config, business_id, updated_at) "
            "SELECT '{}', :bid, NOW() WHERE NOT EXISTS "
            "(SELECT 1 FROM card_config WHERE business_id=:bid)"
        ), {"bid": str(biz.id)})
        db.commit()

        # Send welcome email to new Google-registered businesses
        try:
            dashboard_url = f"{BASE_URL}/biz/{biz.slug}/dashboard"
            send_email(
                to_email=email,
                subject=f"¡Bienvenido a ZubCard, {full_name}! 🎉",
                html_body=f"""
<div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px">
  <h2 style="color:#26170c;font-size:1.4rem;margin-bottom:8px">¡Bienvenido a ZubCard! 🎉</h2>
  <p style="color:#6b5c54;line-height:1.7;margin-bottom:24px">
    Tu cuenta <strong>{full_name}</strong> ya está activa. Accede a tu panel de administración para
    crear tu primera tarjeta de sellos y empezar a fidelizar clientes.
  </p>
  <a href="{dashboard_url}" style="display:inline-block;background:#26170c;color:#fff;padding:13px 28px;border-radius:6px;text-decoration:none;font-weight:700;font-size:.95rem">
    🚀 Ir a mi panel
  </a>
  <p style="color:#a08d83;font-size:.82rem;margin-top:28px;line-height:1.6">
    Puedes iniciar sesión siempre con tu cuenta de Google en <a href="{BASE_URL}/app/login" style="color:#26170c">{BASE_URL}/app/login</a>
  </p>
</div>"""
            )
        except Exception:
            pass

    # Redirect to dashboard via cookie (PIN never visible in URL, history, or server logs)
    response = RedirectResponse(f"/biz/{biz.slug}/dashboard?google=1", status_code=302)
    response.set_cookie(
        "_zc_boot",
        biz.admin_pin,
        max_age=90,
        httponly=False,
        samesite="strict",
        path=f"/biz/{biz.slug}/dashboard",
    )
    return response
