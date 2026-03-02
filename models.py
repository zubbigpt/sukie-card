import uuid
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from database import Base


class Customer(Base):
    __tablename__ = "customers"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    shopify_id   = Column(String, unique=True, nullable=True)
    first_name   = Column(String, nullable=False)
    last_name    = Column(String, nullable=True)
    email        = Column(String, unique=True, nullable=False)
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
