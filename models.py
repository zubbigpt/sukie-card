import uuid
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, Text, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from database import Base


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint("email", "business_id", name="uq_customer_email_business"),
    )

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    shopify_id   = Column(String, unique=True, nullable=True)
    first_name   = Column(String, nullable=False)
    last_name    = Column(String, nullable=True)
    email        = Column(String, nullable=False)
    phone        = Column(String, nullable=True)
    birth_date   = Column(String, nullable=True)   # "YYYY-MM-DD"
    card_active  = Column(Boolean, default=True)
    opt_in       = Column(Boolean, default=True)
    opt_in_email = Column(Boolean, default=True)
    opt_in_sms   = Column(Boolean, default=False)
    # LoyiCard extras
    notes        = Column(Text, nullable=True)         # notas internas
    origin       = Column(String, nullable=True)       # API | Web | Import | Shopify
    channel      = Column(String, nullable=True)       # canal de alta
    language     = Column(String, default="es")
    anniversary_date = Column(String, nullable=True)   # fecha aniversario
    business_id  = Column(UUID(as_uuid=True), ForeignKey("businesses.id"), nullable=True)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    updated_at   = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class LoyaltyCard(Base):
    __tablename__ = "loyalty_cards"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id      = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)
    stamps           = Column(Integer, default=0)    # sellos actuales en tarjeta vigente
    total_stamps     = Column(Integer, default=0)    # acumulados de por vida
    rewards_redeemed = Column(Integer, default=0)    # premios canjeados histórico
    award_balance    = Column(Integer, default=0)    # premios disponibles
    wallet_auth_token = Column(String, nullable=True)  # per-card Apple Wallet auth token (stable)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class StampTransaction(Base):
    __tablename__ = "stamp_transactions"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    card_id          = Column(UUID(as_uuid=True), ForeignKey("loyalty_cards.id"), nullable=False)
    stamps_added     = Column(Integer, nullable=False)
    transaction_type = Column(String, default="stamp")   # stamp | redeem | adjust | register
    note             = Column(Text, nullable=True)
    store            = Column(String, nullable=True)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())


class CardConfig(Base):
    __tablename__ = "card_config"

    id         = Column(Integer, primary_key=True, default=1)
    config     = Column(Text, nullable=False, default="{}")  # JSON blob
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class PushSubscription(Base):
    __tablename__ = "push_subscriptions"
    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    card_id    = Column(UUID(as_uuid=True), ForeignKey("loyalty_cards.id"), nullable=True)
    endpoint   = Column(Text, nullable=False, unique=True)
    p256dh     = Column(Text, nullable=False)
    auth       = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Referral(Base):
    __tablename__ = "referrals"
    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    referrer_card = Column(UUID(as_uuid=True), ForeignKey("loyalty_cards.id"), nullable=False)
    referred_card = Column(UUID(as_uuid=True), ForeignKey("loyalty_cards.id"), nullable=True)
    code          = Column(String(12), unique=True, nullable=False)
    used          = Column(Boolean, default=False)
    bonus_stamps  = Column(Integer, default=2)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    used_at       = Column(DateTime(timezone=True), nullable=True)


class CardProgram(Base):
    """Multi-card / loyalty program configuration for a business."""
    __tablename__ = "card_programs"

    id                    = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id           = Column(UUID(as_uuid=True), ForeignKey("businesses.id"), nullable=True)
    name                  = Column(String, nullable=False)
    emoji                 = Column(String, default="🃏")
    stamps_per_reward     = Column(Integer, default=10)
    reward_name           = Column(String, default="Premio")
    bg_color              = Column(String, default="#0a0a0a")
    accent_color          = Column(String, default="#00e676")
    text_color            = Column(String, default="#ffffff")
    status                = Column(String, default="active")
    sort_order            = Column(Integer, default=0)
    # Email customisation
    welcome_email_subject = Column(String, nullable=True)
    welcome_email_body    = Column(Text, nullable=True)
    # Expiry / visit rules
    expiry_days           = Column(Integer, nullable=True)    # stamps expire after N days (null = never)
    max_stamps_per_visit  = Column(Integer, nullable=True)    # cap per visit (null = unlimited)
    # Strip background image (stored as base64 data URL or external URL)
    strip_bg_url          = Column(Text, nullable=True)
    created_at            = Column(DateTime(timezone=True), server_default=func.now())
    updated_at            = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Business(Base):
    __tablename__ = "businesses"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name            = Column(String, nullable=False)          # "Sukie Cookie"
    slug            = Column(String, unique=True, nullable=False)  # "sukiecookie"
    email           = Column(String, unique=True, nullable=False)  # owner email
    google_id       = Column(String, nullable=True, unique=True)   # Google OAuth sub
    plan            = Column(String, default="free")           # free | pro | enterprise
    card_title      = Column(String, default="Mi Tarjeta")
    stamps_per_reward = Column(Integer, default=10)
    admin_pin           = Column(String, nullable=False)       # random 6-digit internal API token (auto-generated)
    hashed_password     = Column(String, nullable=True)        # bcrypt hash — only for email+password accounts
    email_confirmed     = Column(Boolean, default=False)       # True after clicking confirmation email or Google login
    email_confirm_token = Column(String, nullable=True)        # UUID token sent in confirmation email
    api_key             = Column(String, nullable=False)
    active              = Column(Boolean, default=True)
    logo_url            = Column(String, nullable=True)
    primary_color       = Column(String, default="#3A3426")
    accent_color        = Column(String, default="#FFF5B6")
    industry            = Column(String, nullable=True)        # café | retail | beauty | other
    description         = Column(String, nullable=True)        # free-text "qué vendemos / qué somos"
    # Geo-location for proximity push notifications
    address         = Column(String, nullable=True)
    latitude        = Column(Float, nullable=True)
    longitude       = Column(Float, nullable=True)
    geo_radius_m    = Column(Integer, default=300)
    geo_push_msg    = Column(String, default="¡Estás cerca! Visítanos y acumula sellos 🎉")
    # ── Email branding (per-business) ─────────────────────────────────────────
    # Emails to customers go FROM: "{email_from_name}" <noreply@mail.zubcard.com>
    # with Reply-To: email_reply_to (optional)
    # If the business sets up their own SMTP, those override the global config.
    email_from_name  = Column(String, nullable=True)   # e.g. "Zubbi Cafetería"
    email_reply_to   = Column(String, nullable=True)   # e.g. "hola@zubbi.com"
    email_smtp_host  = Column(String, nullable=True)   # custom SMTP host (optional)
    email_smtp_port  = Column(Integer, nullable=True)  # custom SMTP port
    email_smtp_user  = Column(String, nullable=True)   # custom SMTP user
    email_smtp_pass  = Column(String, nullable=True)   # custom SMTP password
    # ── Google Reviews ─────────────────────────────────────────────────────────
    google_review_url     = Column(String, nullable=True, default="")
    review_trigger_stamps = Column(Integer, default=0)
    # ── Campaign push (promo message shown via Wallet changeMessage) ───────────
    promo_message   = Column(String, nullable=True, default="")
    # ── Stripe billing ─────────────────────────────────────────────────────────
    stripe_customer_id        = Column(String, nullable=True)   # cus_xxx
    stripe_subscription_id    = Column(String, nullable=True)   # sub_xxx
    stripe_subscription_status = Column(String, nullable=True)  # active | past_due | canceled | trialing
    stripe_current_period_end = Column(DateTime(timezone=True), nullable=True)
    # ── Datos fiscales para facturas ───────────────────────────────────────────
    billing_name         = Column(String, nullable=True, default="")   # Razón social
    billing_tax_id       = Column(String, nullable=True, default="")   # CIF/NIF (sin prefijo ES)
    billing_address_line = Column(String, nullable=True, default="")   # Dirección fiscal
    billing_city         = Column(String, nullable=True, default="")
    billing_postal_code  = Column(String, nullable=True, default="")
    billing_country      = Column(String, nullable=True, default="ES")
    # ── Birthday voucher config ────────────────────────────────────────────────
    birthday_gift_type          = Column(String, default="discount")  # discount | product
    birthday_gift_product       = Column(String, nullable=True, default="")
    birthday_email_intro        = Column(String, nullable=True, default="")
    birthday_email_header_color = Column(String, nullable=True, default="")
    birthday_email_accent_color = Column(String, nullable=True, default="")
    birthday_email_banner_url   = Column(String, nullable=True, default="")
    birthday_email_footer_text  = Column(String, nullable=True, default="")
    birthday_email_text_color   = Column(String, nullable=True, default="")   # color del texto del header
    birthday_email_bg_color     = Column(String, nullable=True, default="")   # color de fondo del wrapper
    welcome_email_greeting      = Column(String, nullable=True, default="")   # texto hero bienvenida
    welcome_email_footer        = Column(String, nullable=True, default="")   # pie del email bienvenida
    welcome_email_header_color  = Column(String, nullable=True, default="")   # color fondo cabecera bienvenida
    welcome_email_text_color    = Column(String, nullable=True, default="")   # color texto cabecera bienvenida
    welcome_email_bg_color      = Column(String, nullable=True, default="")   # color fondo wrapper bienvenida
    welcome_email_accent_color  = Column(String, nullable=True, default="")   # color acento bienvenida
    welcome_email_banner_url    = Column(String, nullable=True, default="")   # imagen cabecera bienvenida
    # ── Referral partner (optional) ───────────────────────────────────────────
    referral_partner_id = Column(UUID(as_uuid=True), nullable=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
