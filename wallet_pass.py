"""
Apple Wallet (.pkpass) pass generation for Zubie Card loyalty cards.

Environment variables required (set in Railway):
  APPLE_PASS_TYPE_ID     e.g. "pass.com.zubiecard.loyalty"
  APPLE_TEAM_ID          e.g. "ABC1234DEF"
  APPLE_P12_B64          Base64-encoded .p12 certificate (cert + key bundle)
  APPLE_P12_PASSWORD     Password used when exporting the .p12
  APPLE_WWDR_PEM         (optional) Apple WWDR G4 certificate PEM — defaults to
                         the bundled constant below if not set

Usage:
  from wallet_pass import generate_pkpass
  pkpass_bytes = generate_pkpass(card_id, card_data)
"""

import base64
import hashlib
import io
import json
import os
import zipfile
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_OK = True
except ImportError:
    _PIL_OK = False

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives.serialization.pkcs7 import (
    PKCS7Options,
    PKCS7SignatureBuilder,
)

# ── Apple WWDR G4 certificate (public, expires 2030-12-10) ──────────────────
# Source: https://www.apple.com/certificateauthority/AppleWWDRCAG4.cer
# SHA-256: ea4757885538dd8cb59ff4556f676087d83c85e70902c122e42c0808b5bce14c
APPLE_WWDR_G4_PEM = b"""-----BEGIN CERTIFICATE-----
MIIEVTCCAz2gAwIBAgIUE9x3lVJx5T3GMujM/+Uh88zFztIwDQYJKoZIhvcNAQEL
BQAwYjELMAkGA1UEBhMCVVMxEzARBgNVBAoTCkFwcGxlIEluYy4xJjAkBgNVBAsT
HUFwcGxlIENlcnRpZmljYXRpb24gQXV0aG9yaXR5MRYwFAYDVQQDEw1BcHBsZSBS
b290IENBMB4XDTIwMTIxNjE5MzYwNFoXDTMwMTIxMDAwMDAwMFowdTFEMEIGA1UE
Aww7QXBwbGUgV29ybGR3aWRlIERldmVsb3BlciBSZWxhdGlvbnMgQ2VydGlmaWNh
dGlvbiBBdXRob3JpdHkxCzAJBgNVBAsMAkc0MRMwEQYDVQQKDApBcHBsZSBJbmMu
MQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANAf
eKp6JzKwRl/nF3bYoJ0OKY6tPTKlxGs3yeRBkWq3eXFdDDQEYHX3rkOPR8SGHgjo
v9Y5Ui8eZ/xx8YJtPH4GUnadLLzVQ+mxtLxAOnhRXVGhJeG+bJGdayFZGEHVD41t
QSo5SiHgkJ9OE0/QjJoyuNdqkh4laqQyziIZhQVg3AJK8lrrd3kCfcCXVGySjnYB
5kaP5eYq+6KwrRitbTOFOCOL6oqW7Z+uZk+jDEAnbZXQYojZQykn/e2kv1MukBVl
PNkuYmQzHWxq3Y4hqqRfFcYw7V/mjDaSlLfcOQIA+2SM1AyB8j/VNJeHdSbCb64D
YyEMe9QbsWLFApy9/a8CAwEAAaOB7zCB7DASBgNVHRMBAf8ECDAGAQH/AgEAMB8G
A1UdIwQYMBaAFCvQaUeUdgn+9GuNLkCm90dNfwheMEQGCCsGAQUFBwEBBDgwNjA0
BggrBgEFBQcwAYYoaHR0cDovL29jc3AuYXBwbGUuY29tL29jc3AwMy1hcHBsZXJv
b3RjYTAuBgNVHR8EJzAlMCOgIaAfhh1odHRwOi8vY3JsLmFwcGxlLmNvbS9yb290
LmNybDAdBgNVHQ4EFgQUW9n6HeeaGgujmXYiUIY+kchbd6gwDgYDVR0PAQH/BAQD
AgEGMBAGCiqGSIb3Y2QGAgEEAgUAMA0GCSqGSIb3DQEBCwUAA4IBAQA/Vj2e5bbD
eeZFIGi9v3OLLBKeAuOugCKMBB7DUshwgKj7zqew1UJEggOCTwb8O0kU+9h0UoWv
p50h5wESA5/NQFjQAde/MoMrU1goPO6cn1R2PWQnxn6NHThNLa6B5rmluJyJlPef
x4elUWY0GzlxOSTjh2fvpbFoe4zuPfeutnvi0v/fYcZqdUmVIkSoBPyUuAsuORFJ
EtHlgepZAE9bPFo22noicwkJac3AfOriJP6YRLj477JxPxpd1F1+M02cHSS+APCQ
A1iZQT0xWmJArzmoUUOSqwSonMJNsUvSq3xKX+udO7xPiEAGE/+QF4oIRynoYpgp
pU8RBWk6z/Kf
-----END CERTIFICATE-----
"""

ASSETS_DIR = Path(__file__).parent / "wallet_assets"
BASE_URL = os.environ.get("BASE_URL", "https://app.zubcard.com")

# Font paths (bundled with the server; fall back to default if missing)
_FONT_BOLD = "/usr/share/fonts/truetype/google-fonts/Poppins-Bold.ttf"
_FONT_REG  = "/usr/share/fonts/truetype/google-fonts/Poppins-Regular.ttf"


def generate_strip_image(
    card_name: str = "TARJETA DE FIDELIDAD",
    biz_name: str = "ZubCard",
    customer_name: str = "",
    stamps: int = 0,
    stamps_total: int = 10,
    reward_name: str = "Premio",
    bg_color: str = "#26170c",
    accent_color: str = "#ffca48",
    scale: int = 2,
    strip_bg_bytes: bytes | None = None,
) -> bytes:
    """Generate a loyalty-card strip image for Apple Wallet.
    Clean elegant design: only large stamp circles on background — no text.
    If strip_bg_bytes is provided, uses it as the background image (with a
    semi-transparent color overlay so circles remain visible).
    All text (name, counter, reward) is handled by pass.json fields.
    Returns PNG bytes. Falls back to the static file if PIL is unavailable.

    Official Apple Wallet storeCard strip dimensions:
      strip.png   : 375 × 123 px  (@1x)
      strip@2x.png: 750 × 246 px  (@2x)
      strip@3x.png: 1125 × 369 px (@3x)
    """
    if not _PIL_OK:
        path = ASSETS_DIR / ("strip@2x.png" if scale == 2 else "strip.png")
        if path.exists():
            return path.read_bytes()
        raise RuntimeError("Pillow not available and static strip not found")

    import math as _math

    def _hex(h: str):
        h = h.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

    def _darken(c, amount=0.18):
        return tuple(max(0, int(v * (1 - amount))) for v in c)

    def _lighten(c, amount=0.25):
        return tuple(min(255, int(v + (255 - v) * amount)) for v in c)

    BG     = _hex(bg_color)
    ACCENT = _hex(accent_color)

    EMPTY_BORD  = _lighten(BG, 0.40) if not strip_bg_bytes else (255, 255, 255)
    FILLED_FILL = ACCENT
    FILLED_ICON = _darken(ACCENT, 0.55)

    # Canvas: 375×123 logical px (official Apple Wallet storeCard strip)
    sw, sh = 375 * scale, 123 * scale
    s = scale

    # ── Background layer ──────────────────────────────────────────────────────
    if strip_bg_bytes:
        try:
            bg_img = Image.open(io.BytesIO(strip_bg_bytes)).convert("RGB")
            # Cover: resize so image fills the strip completely
            iw, ih = bg_img.size
            sf = max(sw / iw, sh / ih)
            nw, nh = round(iw * sf), round(ih * sf)
            bg_img = bg_img.resize((nw, nh), Image.LANCZOS)
            left = (nw - sw) // 2
            top  = (nh - sh) // 2
            bg_img = bg_img.crop((left, top, left + sw, top + sh))
            img = bg_img.copy()
            # Semi-transparent BG color overlay for contrast (40% opacity)
            overlay = Image.new("RGBA", (sw, sh), (*BG, 102))
            img.paste(overlay, (0, 0), overlay)
            img = img.convert("RGB")
        except Exception:
            img = Image.new("RGB", (sw, sh), BG)
    else:
        img = Image.new("RGB", (sw, sh), BG)

    draw = ImageDraw.Draw(img, "RGBA")

    # ── Stamp circles: fill the full strip — no text, just circles ───────────
    PAD_H   = round(sw * 0.045)          # horizontal padding
    PAD_V   = round(sh * 0.12)           # vertical padding top & bottom
    avail_w = sw - 2 * PAD_H
    avail_h = sh - 2 * PAD_V

    COLS  = stamps_total if stamps_total <= 5 else _math.ceil(stamps_total / 2)
    ROWS  = _math.ceil(stamps_total / COLS)
    GAP_C = round(avail_w * 0.040)
    GAP_R = round(avail_h * 0.14) if ROWS > 1 else 0
    DOT_D = min(
        (avail_w - GAP_C * (COLS - 1)) // COLS,
        (avail_h - GAP_R * (ROWS - 1)) // max(ROWS, 1),
    )
    DOT_D = max(DOT_D, 4 * s)
    DOT_R = DOT_D // 2

    grid_w = COLS * DOT_D + GAP_C * (COLS - 1)
    grid_h = ROWS * DOT_D + GAP_R * (ROWS - 1)
    gx0    = PAD_H + (avail_w - grid_w) // 2
    gy0    = PAD_V + (avail_h - grid_h) // 2

    for i in range(stamps_total):
        col = i % COLS
        row = i // COLS
        cx  = gx0 + col * (DOT_D + GAP_C) + DOT_R
        cy  = gy0 + row * (DOT_D + GAP_R) + DOT_R
        r   = DOT_R

        if i < stamps:
            # Filled: solid accent circle with checkmark
            draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(*FILLED_FILL, 255))
            lw = max(2, round(r * 0.16))
            x1 = cx - int(r * 0.30); y1 = cy + int(r * 0.06)
            x2 = cx - int(r * 0.02); y2 = cy + int(r * 0.36)
            x3 = cx + int(r * 0.38); y3 = cy - int(r * 0.26)
            draw.line([(x1, y1), (x2, y2)], fill=(*FILLED_ICON, 245), width=lw)
            draw.line([(x2, y2), (x3, y3)], fill=(*FILLED_ICON, 245), width=lw)
        else:
            # Empty: outline only — clean, no fill
            draw.ellipse([cx - r, cy - r, cx + r, cy + r],
                         outline=(*EMPTY_BORD, 150 if strip_bg_bytes else 130),
                         width=max(2, s))

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()


def _sha1_file(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _load_certificates():
    """Load signing credentials from environment variables."""
    p12_b64 = os.environ.get("APPLE_P12_B64", "")
    p12_pass = os.environ.get("APPLE_P12_PASSWORD", "").encode()
    if not p12_b64:
        raise ValueError(
            "APPLE_P12_B64 environment variable not set. "
            "Export your Pass Type certificate as .p12 and base64-encode it."
        )
    p12_data = base64.b64decode(p12_b64)
    private_key, certificate, _ = pkcs12.load_key_and_certificates(p12_data, p12_pass)

    # WWDR cert
    wwdr_pem = os.environ.get("APPLE_WWDR_PEM", "").encode() or APPLE_WWDR_G4_PEM
    wwdr_cert = x509.load_pem_x509_certificate(wwdr_pem)

    return private_key, certificate, wwdr_cert


def _sign_manifest(manifest_data: bytes, private_key, certificate, wwdr_cert) -> bytes:
    """Create a detached CMS / PKCS#7 signature over manifest_data."""
    builder = PKCS7SignatureBuilder()
    builder = builder.set_data(manifest_data)
    builder = builder.add_signer(certificate, private_key, hashes.SHA256())
    builder = builder.add_certificate(wwdr_cert)
    return builder.sign(
        serialization.Encoding.DER,
        [PKCS7Options.DetachedSignature],
    )


def build_pass_json(
    card_id: str,
    first_name: str,
    last_name: str,
    stamps: int,
    stamps_per_reward: int,
    reward_name: str,
    biz_name: str,
    primary_color: str = "#26170c",
    accent_color: str = "#ffca48",
    text_color: str = "#ffffff",
    auth_token: str = "",
    latitude: float | None = None,
    longitude: float | None = None,
    geo_push_msg: str = "",
    geo_radius_m: int = 300,
) -> dict:
    """Build the pass.json dict for a loyalty card."""
    pass_type_id = os.environ.get("APPLE_PASS_TYPE_ID", "pass.com.zubiecard.loyalty")
    team_id = os.environ.get("APPLE_TEAM_ID", "")

    # Convert hex to rgb string for Apple Wallet
    def hex_to_rgb(h: str) -> str:
        h = h.lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgb({r},{g},{b})"

    bg = hex_to_rgb(primary_color)
    fg = hex_to_rgb(accent_color)
    tc = hex_to_rgb(text_color or "#ffffff")

    full_name = f"{first_name} {last_name}".strip() or "Cliente"
    serial = str(card_id).replace("-", "")[:20]

    pass_dict = {
        "formatVersion": 1,
        "passTypeIdentifier": pass_type_id,
        "serialNumber": serial,
        "teamIdentifier": team_id,
        "organizationName": biz_name or "Zubie Card",
        "description": "Tarjeta de Fidelidad",
        "logoText": (biz_name or "ZubCard")[:20],
        "backgroundColor": bg,
        "foregroundColor": tc,       # user-chosen text color
        "labelColor": fg,            # accent color for field labels
        "storeCard": {
            "headerFields": [
                {
                    "key": "stamps",
                    "label": "SELLOS",
                    "value": f"{stamps}/{stamps_per_reward}",
                    "changeMessage": "Tienes %@ sellos",
                    "textAlignment": "PKTextAlignmentRight",
                }
            ],
            "secondaryFields": [
                {
                    "key": "holder",
                    "label": "TITULAR",
                    "value": full_name,
                    "textAlignment": "PKTextAlignmentLeft",
                },
                {
                    "key": "reward",
                    "label": "PREMIO",
                    "value": reward_name or "Premio",
                    "textAlignment": "PKTextAlignmentRight",
                },
            ],
            "backFields": [
                {
                    "key": "card_url",
                    "label": "Ver tarjeta digital",
                    "value": f"{BASE_URL}/card/{card_id}",
                    "attributedValue": f"<a href='{BASE_URL}/card/{card_id}'>Abrir tarjeta</a>",
                },
                {
                    "key": "info",
                    "label": "Información",
                    "value": f"Por cada {stamps_per_reward} sellos obtienes {reward_name}. "
                             "Presenta esta tarjeta en caja en tu próxima visita.",
                },
            ],
        },
        "barcode": {
            "message": str(card_id),
            "format": "PKBarcodeFormatQR",
            "messageEncoding": "iso-8859-1",
            "altText": f"Card {serial[:8].upper()}",
        },
        "barcodes": [
            {
                "message": str(card_id),
                "format": "PKBarcodeFormatQR",
                "messageEncoding": "iso-8859-1",
                "altText": f"Card {serial[:8].upper()}",
            }
        ],
    }

    # Add web service for live updates if configured
    if BASE_URL and auth_token:
        pass_dict["webServiceURL"] = f"{BASE_URL}/api/wallet/"
        pass_dict["authenticationToken"] = auth_token

    # ── iOS native proximity notification ────────────────────────────────────
    # When the pass holder comes within ~100m (or geo_radius_m, max 1000m per
    # Apple spec) of these coordinates, iOS shows a lock-screen notification
    # with the relevantText — no server, no push certificate needed.
    if latitude is not None and longitude is not None:
        relevant_text = geo_push_msg.strip() if geo_push_msg else f"¡Estás cerca de {biz_name}! Visítanos y acumula sellos 🎉"
        # Apple Wallet maxDistance max is ~1000m; clamp to that
        max_dist = min(int(geo_radius_m) if geo_radius_m else 300, 1000)
        pass_dict["locations"] = [
            {
                "latitude":     float(latitude),
                "longitude":    float(longitude),
                "relevantText": relevant_text,
                "maxDistance":  max_dist,
            }
        ]

    return pass_dict


def generate_pkpass(
    card_id: str,
    first_name: str = "Cliente",
    last_name: str = "",
    stamps: int = 0,
    stamps_per_reward: int = 10,
    reward_name: str = "Premio",
    biz_name: str = "ZubCard",
    primary_color: str = "#26170c",
    accent_color: str = "#ffca48",
    text_color: str = "#ffffff",
    latitude: float | None = None,
    longitude: float | None = None,
    geo_push_msg: str = "",
    geo_radius_m: int = 300,
    strip_bg_url: str = "",
) -> bytes:
    """
    Generate a signed .pkpass file and return it as bytes.
    Raises ValueError if Apple certificates are not configured.
    strip_bg_url: base64 data URL or https URL for the strip background image.
    """
    private_key, certificate, wwdr_cert = _load_certificates()

    # 1. Build pass.json
    pass_data = build_pass_json(
        card_id=card_id,
        first_name=first_name,
        last_name=last_name,
        stamps=stamps,
        stamps_per_reward=stamps_per_reward,
        reward_name=reward_name,
        biz_name=biz_name,
        primary_color=primary_color,
        accent_color=accent_color,
        text_color=text_color,
        latitude=latitude,
        longitude=longitude,
        geo_push_msg=geo_push_msg,
        geo_radius_m=geo_radius_m,
    )
    pass_json_bytes = json.dumps(pass_data, ensure_ascii=False, indent=2).encode("utf-8")

    # 2. Load image assets (strip is generated dynamically)
    assets: dict[str, bytes] = {}
    for fname in ["icon.png", "icon@2x.png", "icon@3x.png", "logo.png", "logo@2x.png"]:
        asset_path = ASSETS_DIR / fname
        if asset_path.exists():
            assets[fname] = asset_path.read_bytes()

    # Decode strip background image if provided
    _strip_bg_bytes: bytes | None = None
    if strip_bg_url:
        try:
            if strip_bg_url.startswith("data:"):
                _b64_part = strip_bg_url.split(",", 1)[1]
                _strip_bg_bytes = base64.b64decode(_b64_part)
            else:
                import urllib.request as _urlreq
                with _urlreq.urlopen(strip_bg_url, timeout=5) as _r:
                    _strip_bg_bytes = _r.read()
        except Exception:
            _strip_bg_bytes = None  # fall back to solid color background

    # Dynamic strip: clean minimal design
    customer_name = f"{first_name} {last_name}".strip()
    try:
        assets["strip.png"]   = generate_strip_image(
            card_name="TARJETA DE FIDELIDAD", biz_name=biz_name,
            customer_name=customer_name,
            stamps=stamps, stamps_total=stamps_per_reward,
            reward_name=reward_name, bg_color=primary_color,
            accent_color=accent_color, scale=1,
            strip_bg_bytes=_strip_bg_bytes)
        assets["strip@2x.png"] = generate_strip_image(
            card_name="TARJETA DE FIDELIDAD", biz_name=biz_name,
            customer_name=customer_name,
            stamps=stamps, stamps_total=stamps_per_reward,
            reward_name=reward_name, bg_color=primary_color,
            accent_color=accent_color, scale=2,
            strip_bg_bytes=_strip_bg_bytes)
    except Exception as _e:
        # Fall back to static strip if generation fails
        for fname in ["strip.png", "strip@2x.png"]:
            asset_path = ASSETS_DIR / fname
            if asset_path.exists():
                assets[fname] = asset_path.read_bytes()

    # 3. Build manifest.json (sha1 of every file in the pass)
    all_files: dict[str, bytes] = {"pass.json": pass_json_bytes, **assets}
    manifest = {name: _sha1_file(data) for name, data in all_files.items()}
    manifest_bytes = json.dumps(manifest, ensure_ascii=False).encode("utf-8")

    # 4. Sign the manifest
    signature_bytes = _sign_manifest(manifest_bytes, private_key, certificate, wwdr_cert)

    # 5. Bundle into a zip (.pkpass)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("pass.json", pass_json_bytes)
        zf.writestr("manifest.json", manifest_bytes)
        zf.writestr("signature", signature_bytes)
        for fname, data in assets.items():
            zf.writestr(fname, data)

    return buf.getvalue()
