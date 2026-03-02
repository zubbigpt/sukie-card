# 🍪 Sukie Card — Sistema propio de fidelización

Sistema de tarjetas de fidelidad propio para Sukie Cookie.
**Sin LoyiCard. Sin terceros. 100% tuyo.**

---

## Cómo funciona

1. **Cliente nuevo en Shopify** → Make.com lo detecta → Llama a nuestra API → Se crea su tarjeta
2. **Cliente recibe email** con enlace a su tarjeta digital (con QR)
3. **Cliente compra** → Muestra su QR → Sukie escanea → Añade los sellos
4. **A los 10 sellos** → ¡Galleta gratis! → Sukie canjea el premio en el mismo panel

---

## Pasos de deployment en Railway

### 1. Crear repositorio en GitHub
```bash
cd sukie-card-system
git init
git add .
git commit -m "Initial Sukie Card system"
# Crea el repo en github.com y luego:
git remote add origin https://github.com/TU_USUARIO/sukie-card.git
git push -u origin main
```

### 2. Crear proyecto en Railway
1. Ve a [railway.app](https://railway.app)
2. "New Project" → "Deploy from GitHub repo" → selecciona `sukie-card`
3. Railway detecta Python automáticamente

### 3. Añadir PostgreSQL
1. En tu proyecto Railway → "New Service" → "Database" → "PostgreSQL"
2. Railway añade `DATABASE_URL` automáticamente a tu servicio

### 4. Configurar variables de entorno
En Railway → tu servicio → "Variables":

| Variable | Valor |
|----------|-------|
| `BASE_URL` | `https://TU-APP.up.railway.app` (Railway te da esta URL) |
| `ADMIN_PIN` | Elige un PIN de 4 dígitos (ej: 5678) |
| `API_KEY` | Una clave secreta larga (ej: sukie-cookie-2026-secret) |
| `STAMPS_PER_REWARD` | 10 |

### 5. Deploy
Railway hace el deploy automáticamente al hacer push a GitHub.

---

## URLs del sistema

| URL | Función |
|-----|---------|
| `https://TU-APP.up.railway.app/card/{id}` | Tarjeta del cliente (con QR) |
| `https://TU-APP.up.railway.app/admin` | Panel admin (con PIN) |
| `https://TU-APP.up.railway.app/api/cards` | API para Make.com |
| `https://TU-APP.up.railway.app/health` | Health check |

---

## Configurar Make.com (Flow 6)

Añade un módulo **HTTP "Make a Request"** entre el módulo Shopify y el módulo Gmail:

- **URL:** `https://TU-APP.up.railway.app/api/cards`
- **Method:** POST
- **Headers:** `Authorization: Bearer TU_API_KEY`
- **Body (JSON):**
```json
{
  "email": "{{1.email}}",
  "first_name": "{{1.first_name}}",
  "last_name": "{{1.last_name}}",
  "shopify_id": "{{1.id}}"
}
```

El módulo devuelve `card_url` — úsalo en el email de bienvenida como enlace del botón.

---

## Eliminar LoyiCard de Make.com

1. Busca en Make.com cualquier escenario que tenga módulos de LoyiCard
2. Elimínalos o desactívalos
3. También elimina Flow 7 (ya no lo necesitamos — nuestro sistema crea las tarjetas solo)
