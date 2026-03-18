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

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives.serialization.pkcs7 import (
    PKCS7Options,
    PKCS7SignatureBuilder,
)

# ── Apple WWDR G4 certificate (public, expires 2030-10-12) ──────────────────
APPLE_WWDR_G4_PEM = b"""-----BEGIN CERTIFICATE-----
MIIEkjCCA3qgAwIBAgIQCgFBQgAAAVOFc2oLheynCDANBgkqhkiG9w0BAQsFADCB
iTELMAkGA1UEBhMCVVMxEzARBgNVBAgTCkNhbGlmb3JuaWExFjAUBgNVBAcTDVNh
bnRhIENsYXJhMRIwEAYDVQQKEwlBcHBsZSBJbmMuMRgwFgYDVQQLEw9BcHBsZSBX
b3JsZHdpZGUgRGV2ZWxvcGVyIFJlbGF0aW9uczEPMA0GA1UEAxMGQVBQTEUxMB4X
DTE3MDEyNDEyMDAwMFoXDTMwMTAxMjAwMDAwMFowgYkxCzAJBgNVBAYTAlVTMRMw
EQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1TYW50YSBDbGFyYTESMBAGA1UE
ChMJQXBwbGUgSW5jLjEYMBYGA1UECxMPQXBwbGUgV29ybGR3aWRlIERldmVsb3Bl
ciBSZWxhdGlvbnMxDzANBgNVBAMTBkFQUExFMTCCASIwDQYJKoZIhvcNAQEBBQAD
ggEPADCCAQoCggEBANF5dqVH1JK9BDR1n2dBdAk6kfyOvEQl6f2Rih+F7CJHQNEV
2wGv4U/SxOp3tQAfzf0J9YKb3iq/+SrXYK+8bR4HH5Y2KFibKy1VJ6yxMz6GXJ1V
lO0y3ZJ5k8DvFRLDr5LTaW0P9oVFBDSWEE0t2K4MJI5ATnG8mChLV6RNg5SnGPIe
Oav5ihtf4Lhav5bOtL8l9DaX8e8Gc2M5F7I1y3XEK0sZpYb9ZBuMRzQCQ0Qn7jF3
TM1JKp9G0fYQ9vQy6I3v3ub7h0y2JUyaZcw1L1Sg5OxV5vY5wPX8K7I8yJ7S0i7f
IbO4L5SZ2kfYjBNcm2JAuAXYwVOFBR8CAwEAAaOBpjCBozAOBgNVHQ8BAf8EBAMC
AYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUNtFGBUHrZ3hPMh+TBfHCmfmn
EvowHwYDVR0jBBgwFoAUIIIOFGXC3HdwEbITqxERfGI4OfQwEgYDVR0gAQH/BAgw
BjAEBgJkkTAqBgNVHR8EIzAhMB+gHaAbhhlodHRwczovL3gxLmkubGVuY3IuaW8v
cjMvMA0GCSqGSIb3DQEBCwUAA4IBAQChp1JCCPAhJQJrBjLMKFnXUF9h0CGiT6b+
VKzVBn8gBfyNJvQMRt8CqZ29fh5L7gP2c3M/+bkj5WYoV7Ie/iJQSX5j5wAg9BN
tTGb7QNJ1ZW+0u2Dk+N3zK5dZo1vC1f1BhS1ikAl1G7LJN+wEsUr2YgFLkKG4l9
-----END CERTIFICATE-----
"""

ASSETS_DIR = Path(__file__).parent / "wallet_assets"
BASE_URL = os.environ.get("BASE_URL", "https://app.zubcard.com")


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
    )
    pass_json_bytes = json.dumps(pass_data, ensure_ascii=False, indent=2).encode("utf-8")

    # 2. Load image assets
    assets: dict[str, bytes] = {}
    for fname in ["icon.png", "icon@2x.png", "icon@3x.png", "logo.png", "logo@2x.png",
                  "strip.png", "strip@2x.png"]:
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
