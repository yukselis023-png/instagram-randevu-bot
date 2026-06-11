# VPS Deployment Guide — instagram-randevu-bot

## Architecture (Hostinger VPS)

```
Hostinger VPS (Ubuntu 22.04)
├── Booking API (FastAPI, Gunicorn + Uvicorn)
├── PostgreSQL 16
├── n8n (workflow engine)
├── Instagram Poller(s) (one per tenant)
├── antigravity_tools (LLM proxy → Gemini)
├── cloudflared (tunnel for webhooks)
└── Nginx (reverse proxy + SSL)
        ↕
CRM Frontend (Vercel) → points to VPS API URL
```

## Prerequisites

- Hostinger VPS (min 2GB RAM, 2 CPU)
- Domain name (e.g., api.business.com)
- Cloudflare account (for tunnel/SSL)
- GitHub access (yukselis023-png/instagram-randevu-bot)

## Step 1: Initial VPS Setup

```bash
ssh root@YOUR_VPS_IP
apt update && apt upgrade -y
apt install -y docker.io docker-compose nginx certbot python3-pip
systemctl enable docker && systemctl start docker
```

## Step 2: Environment File

Create `/opt/instagram-bot/.env`:

```env
# Database
POSTGRES_PASSWORD=<generate-random>
POSTGRES_DB=instagram_booking

# LLM (antigravity_tools runs locally)
LLM_BASE_URL=http://127.0.0.1:8045/v1
LLM_API_KEY=sk-93ac4612b7b5427d9de03ec1b96e8f26
LLM_MODEL=gemini-3-flash

# Business config
BUSINESS_NAME=DOEL Digital
TIMEZONE=Europe/Istanbul
WORKING_HOURS_START=10:00
WORKING_HOURS_END=19:00
SLOT_DURATION_MINUTES=60
SLOT_BUFFER_MINUTES=10
APPOINTMENT_LOOKAHEAD_DAYS=30
DEFAULT_SERVICE_NAME="Strateji Görüşmesi"

# CRM (Supabase - stays same regardless of VPS)
LIVE_CRM_ENABLED=true
LIVE_CRM_SUPABASE_URL=https://rnjkilyiqnqiyqhwqdly.supabase.co
LIVE_CRM_SUPABASE_ANON_KEY=sb_publishable_...
LIVE_CRM_EMAIL=infodoeldigital+crm@gmail.com
LIVE_CRM_PASSWORD=...

# n8n
N8N_ENCRYPTION_KEY=<generate-random>
N8N_BASIC_AUTH_USER=admin
N8N_BASIC_AUTH_PASSWORD=<generate-random>

# CORS (CRM frontend URL)
CORS_ALLOW_ORIGINS=https://doel-crm.vercel.app,https://YOUR_DOMAIN
```

## Step 3: Docker Compose

Create `/opt/instagram-bot/docker-compose.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16-alpine
    volumes:
      - pgdata:/var/lib/postgresql/data
    env_file: .env
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  booking-api:
    build: ./booking-api
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
    command: >
      sh -c "cd /app &&
             alembic upgrade head || true &&
             uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2"

  n8n:
    image: n8nio/n8n:latest
    ports:
      - "5678:5678"
    env_file: .env
    volumes:
      - n8n_data:/home/node/.n8n

  cloudflared:
    image: cloudflare/cloudflared:latest
    command: tunnel --url http://booking-api:8000
    restart: unless-stopped

  antigravity:
    image: python:3.11-slim
    command: python -m antigravity_tools --port 8045
    ports:
      - "8045:8045"
    restart: unless-stopped
    # Alternative: run antigravity_tools binary directly

volumes:
  pgdata:
  n8n_data:
```

## Step 4: Nginx Reverse Proxy

```nginx
# /etc/nginx/sites-available/api.YOUR_DOMAIN
server {
    listen 80;
    server_name api.YOUR_DOMAIN;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
    }
}
```

Then:
```bash
certbot --nginx -d api.YOUR_DOMAIN
ln -s /etc/nginx/sites-available/api.YOUR_DOMAIN /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
```

## Step 5: Deploy

```bash
cd /opt/instagram-bot
git clone https://github.com/yukselis023-png/instagram-randevu-bot.git .
docker compose up -d --build
```

## Step 6: Update CRM Frontend

In Vercel dashboard for `doel-crm-source`:
- Set `VITE_CRM_API_BASE_URL` → `https://api.YOUR_DOMAIN`
- Set `VITE_AI_AGENT_API_BASE_URL` → `https://api.YOUR_DOMAIN`

Or set these in `.env.production` and redeploy.

## Step 7: Verify

```bash
curl https://api.YOUR_DOMAIN/health
curl https://api.YOUR_DOMAIN/version
```

## Multi-Tenant Instagram Pollers

For each tenant, run a separate poller container:

```bash
docker run -d \
  --name poller-TENANT_SLUG \
  --env IG_LOGIN_USERNAME=tenant_ig_user \
  --env IG_LOGIN_PASSWORD=tenant_ig_pass \
  --env TENANT_SLUG=TENANT_SLUG \
  --network instagram-bot_default \
  instagram-randevu-bot_poller
```

## Key Differences from Render

| Aspect | Render | VPS |
|--------|--------|-----|
| Cold start | ~30s (free tier) | Instant |
| Workers | 1 (free) | Multi-worker |
| PostgreSQL | Managed (free, 1GB) | Full control |
| SSL | Auto (onrender.com) | certbot/Cloudflare |
| Custom domain | No (free tier) | Yes |
| Multi-poller | Not possible | Multiple containers |
| Cost | Free (limited) | ~$10-15/mo |
