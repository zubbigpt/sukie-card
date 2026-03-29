# ZubCard — Memoria de Claude

## REGLA: Usar CLI, NO Chrome

Cuando trabajes con Railway o GitHub en este proyecto, **siempre usar CLI/API directa**.

---

## Setup de CLI (hacer al inicio de cada sesión)

```bash
# Railway CLI (ARM64 Linux)
curl -fsSL "https://github.com/railwayapp/cli/releases/download/v4.35.0/railway-v4.35.0-aarch64-unknown-linux-musl.tar.gz" \
  -o /tmp/railway.tar.gz && tar xzf /tmp/railway.tar.gz -C /tmp && chmod +x /tmp/railway
mkdir -p /sessions/great-peaceful-volta/bin && mv /tmp/railway /sessions/great-peaceful-volta/bin/

# GitHub CLI (ARM64 Linux)
curl -fsSL https://github.com/cli/cli/releases/download/v2.67.0/gh_2.67.0_linux_arm64.tar.gz \
  -o /tmp/gh.tar.gz && tar xzf /tmp/gh.tar.gz -C /tmp
mv /tmp/gh_2.67.0_linux_arm64/bin/gh /sessions/great-peaceful-volta/bin/

export PATH="/sessions/great-peaceful-volta/bin:$PATH"

# Autenticar GitHub — pedirle a Sukie que corra en su Mac: gh auth token
echo "TOKEN_DE_SUKIE" | gh auth login --with-token

# Crear helper de logs Railway
cat > /sessions/great-peaceful-volta/bin/railway-logs << 'SCRIPT'
#!/bin/bash
# Pedir token Railway a Sukie (railway.app → Account → Tokens)
TOKEN="RAILWAY_API_TOKEN"
DEPLOYMENT_ID="${1:-$(curl -s -X POST https://backboard.railway.app/graphql/v2 \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"{ deployments(input: { serviceId: \"effd8945-c6e9-4fd3-aaf0-1996505a8538\" }) { edges { node { id } } } }"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['deployments']['edges'][0]['node']['id'])")}"
FILTER="${2:-}"
curl -s -X POST https://backboard.railway.app/graphql/v2 \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"query\":\"{ deploymentLogs(deploymentId: \\\"$DEPLOYMENT_ID\\\", filter: \\\"$FILTER\\\", limit: 100) { message timestamp } }\"}" \
  | python3 -c "
import json,sys
for l in (json.load(sys.stdin).get('data',{}).get('deploymentLogs',[]) or []):
    print(l.get('timestamp','')[:19], l.get('message',''))
"
SCRIPT
chmod +x /sessions/great-peaceful-volta/bin/railway-logs
```

---

## IDs de Railway (no son secretos)

| Recurso | ID |
|---------|-----|
| Project | 8023e542-746d-48a8-becb-80add00a541c |
| Service (web) | effd8945-c6e9-4fd3-aaf0-1996505a8538 |
| Environment | 1ee38bab-bfa6-4863-b8d5-5360033a7f0c |

## Nota: Railway CLI v4 no acepta tokens en config file

Para logs usar la API GraphQL directamente (helper `railway-logs`).
Para deployar, usar `git push origin main` — Railway auto-deploya en ~2 min.

---

## Stack

- **Backend**: FastAPI — `main.py` + `wallet_pass.py`
- **DB**: PostgreSQL en Railway
- **Email**: Resend API (`send.zubcard.com`)
- **Dominio**: zubcard.com (Namecheap)
- **Repo**: github.com/zubbigpt/sukie-card
