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
    card_name: str = "FIDELITY CARD",
    biz_name: str = "Sukie Card",
    stamps: int = 0,
    stamps_total: int = 8,
    reward_name: str = "Premio",
    bg_color: str = "#26170c",
    accent_color: str = "#ffca48",
    scale: int = 2,
) -> bytes:
    """Generate a loyalty-card strip image for Apple Wallet.
    Clean tile-style design: solid bg, square stamp tiles with checkmark,
    chip top-right, TITULAR/PREMIOS footer.
    Returns PNG bytes. Falls back to the static file if PIL is unavailable."""
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
    WHITE  = (255, 255, 255)

    EMPTY_FILL  = _darken(BG, 0.15)
    FILLED_FILL = ACCENT
    FILLED_ICON = _darken(ACCENT, 0.45)
    TEXT_MAIN   = _lighten(BG, 0.85)
    TEXT_SUB    = _lighten(BG, 0.45)

    sw, sh = 320 * scale, 123 * scale
    s = scale

    img  = Image.new("RGB", (sw, sh), BG)
    draw = ImageDraw.Draw(img, "RGBA")

    def _fnt(path, size):
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            return ImageFont.load_default()

    fnt_label  = _fnt(_FONT_REG,  8 * s)
    fnt_name   = _fnt(_FONT_BOLD, 16 * s)
    fnt_small  = _fnt(_FONT_REG,  8 * s)
    fnt_bold_s = _fnt(_FONT_BOLD, 9 * s)

    PAD   = 14 * s
    PAD_T = 10 * s

    # ── LOYALTY CARD label ──────────────────────────────────────
    draw.text((PAD, PAD_T), "LOYALTY CARD",
              font=fnt_label, fill=(*TEXT_SUB, 255))

    # ── Card name ───────────────────────────────────────────────
    draw.text((PAD, PAD_T + 10 * s), card_name.upper()[:20],
              font=fnt_name, fill=(*TEXT_MAIN, 255))

    # ── Chip (top-right, simple rounded rect with lines) ────────
    cw, ch_c = 22 * s, 16 * s
    chip_x = sw - PAD - cw
    chip_y = PAD_T
    draw.rounded_rectangle(
        [chip_x, chip_y, chip_x + cw, chip_y + ch_c],
        radius=3 * s, fill=(*ACCENT, 255)
    )
    line_col = (*_darken(ACCENT, 0.4), 200)
    draw.line([(chip_x + 7*s, chip_y + 2), (chip_x + 7*s,  chip_y + ch_c - 2)],
              fill=line_col, width=max(1, s))
    draw.line([(chip_x + 15*s, chip_y + 2), (chip_x + 15*s, chip_y + ch_c - 2)],
              fill=line_col, width=max(1, s))
    draw.line([(chip_x + 2, chip_y + 8*s), (chip_x + cw - 2, chip_y + 8*s)],
              fill=line_col, width=max(1, s))

    # ── Stamp grid ──────────────────────────────────────────────
    FOOTER_H     = 28 * s
    grid_top     = PAD_T + 10 * s + 18 * s + 4 * s
    grid_bottom  = sh - FOOTER_H - 6 * s
    grid_h_avail = grid_bottom - grid_top
    grid_w_avail = sw - 2 * PAD

    COLS = min(stamps_total, 5)
    ROWS = _math.ceil(stamps_total / COLS)

    GAP          = 5 * s
    tile_from_w  = (grid_w_avail - GAP * (COLS - 1)) // COLS
    tile_from_h  = (grid_h_avail - GAP * (ROWS - 1)) // ROWS
    tile         = min(tile_from_w, tile_from_h, 22 * s)
    tile         = max(tile, 12 * s)

    grid_w = COLS * tile + GAP * (COLS - 1)
    grid_h = ROWS * tile + GAP * (ROWS - 1)
    gx0    = PAD + (grid_w_avail - grid_w) // 2
    gy0    = grid_top + (grid_h_avail - grid_h) // 2

    radius = max(3 * s, tile // 5)

    def _draw_check(cx, cy, size, color):
        lw = max(2, size // 7)
        x1 = cx - int(size * 0.28); y1 = cy + int(size * 0.02)
        x2 = cx - int(size * 0.04); y2 = cy + int(size * 0.24)
        x3 = cx + int(size * 0.32); y3 = cy - int(size * 0.24)
        draw.line([(x1, y1), (x2, y2)], fill=(*color, 230), width=lw)
        draw.line([(x2, y2), (x3, y3)], fill=(*color, 230), width=lw)

    for i in range(stamps_total):
        col = i % COLS
        row = i // COLS
        x   = gx0 + col * (tile + GAP)
        y   = gy0 + row * (tile + GAP)
        cx2 = x + tile // 2
        cy2 = y + tile // 2

        if i < stamps:
            draw.rounded_rectangle([x, y, x + tile, y + tile],
                                   radius=radius, fill=(*FILLED_FILL, 255))
            _draw_check(cx2, cy2, tile, FILLED_ICON)
        else:
            draw.rounded_rectangle([x, y, x + tile, y + tile],
                                   radius=radius, fill=(*EMPTY_FILL, 255))

    # ── Footer ──────────────────────────────────────────────────
    footer_y = sh - FOOTER_H
    draw.line([(PAD, footer_y), (sw - PAD, footer_y)],
              fill=(*TEXT_SUB, 80), width=max(1, s))
    lbl_y = footer_y + 4 * s
    val_y = footer_y + 13 * s
    draw.text((PAD, lbl_y), "TITULAR",
              font=fnt_small, fill=(*TEXT_SUB, 200))
    draw.text((PAD, val_y), biz_name.upper()[:22],
              font=fnt_bold_s, fill=(*TEXT_MAIN, 255))
    draw.text((sw - PAD, lbl_y), "PREMIOS",
              font=fnt_small, fill=(*TEXT_SUB, 200), anchor="ra")
    draw.text((sw - PAD, val_y), reward_name[:20],
              font=fnt_bold_s, fill=(*ACCENT, 230), anchor="ra")

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
        [PKCS7Options.DetachedSignature, PKCS7Options.NoCerts],
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

    full_name = f"{first_name} {last_name}".strip() or "Cliente"
    serial = str(card_id).replace("-", "")[:20]

    pass_dict = {
        "formatVersion": 1,
        "passTypeIdentifier": pass_type_id,
        "serialNumber": serial,
        "teamIdentifier": team_id,
        "organizationName": biz_name or "Zubie Card",
        "description": f"Tarjeta de Fidelidad · {biz_name or 'Zubie Card'}",
        "logoText": (biz_name or "ZUBIECARD").upper()[:20],
        "backgroundColor": bg,
        "foregroundColor": "rgb(255,243,208)",
        "labelColor": "rgb(160,141,131)",
        "storeCard": {
            "primaryFields": [
                {
                    "key": "stamps",
                    "label": "SELLOS",
                    "value": f"{stamps}/{stamps_per_reward}",
                    "changeMessage": "Tienes %@ sellos",
                }
            ],
            "secondaryFields": [
                {
                    "key": "reward",
                    "label": "PREMIO",
                    "value": reward_name or "Premio",
                }
            ],
            "auxiliaryFields": [
                {
                    "key": "holder",
                    "label": "TITULAR",
                    "value": full_name,
                }
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
    biz_name: str = "Zubie Card",
    primary_color: str = "#26170c",
    accent_color: str = "#ffca48",
    latitude: float | None = None,
    longitude: float | None = None,
    geo_push_msg: str = "",
    geo_radius_m: int = 300,
) -> bytes:
    """
    Generate a signed .pkpass file and return it as bytes.
    Raises ValueError if Apple certificates are not configured.
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

    # Dynamic strip: dark FIDELITY CARD design with stamp grid
    try:
        assets["strip.png"]   = generate_strip_image(
            card_name="FIDELITY CARD", biz_name=biz_name,
            stamps=stamps, stamps_total=stamps_per_reward,
            reward_name=reward_name, bg_color=primary_color,
            accent_color=accent_color, scale=1)
        assets["strip@2x.png"] = generate_strip_image(
            card_name="FIDELITY CARD", biz_name=biz_name,
            stamps=stamps, stamps_total=stamps_per_reward,
            reward_name=reward_name, bg_color=primary_color,
            accent_color=accent_color, scale=2)
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
