# Render Deploy Rehberi

## 1. Backend servis
Render üzerinde yeni bir **Web Service** oluştur.
- Runtime: Docker
- Root Directory: `booking-api`
- Health Check Path: `/health`
- Port env: `8000`

## 2. Gerekli env listesi
Örnek env listesi:
- `booking-api/RENDER_ENV.example`

## 3. Kritik env'ler
- `DATABASE_URL` — PostgreSQL
- `LLM_BASE_URL` — LLM API endpoint
- `LLM_API_KEY` — LLM API key
- `CRM_SUPABASE_URL`
- `CRM_SUPABASE_ANON_KEY`
- `CRM_SUPABASE_EMAIL`
- `CRM_SUPABASE_PASSWORD`
- `CRM_WORKSPACE_ID`
- `LIVE_CRM_SUPABASE_URL`
- `LIVE_CRM_SUPABASE_ANON_KEY`
- `LIVE_CRM_EMAIL`
- `LIVE_CRM_PASSWORD`
- `CORS_ALLOW_ORIGINS=https://doel-crm.vercel.app`
- `TELEGRAM_BOT_TOKEN` (opsiyonel, handoff bildirimleri için)
- `TELEGRAM_HANDOFF_CHAT_ID` (opsiyonel)
- `WHATSAPP_ACCESS_TOKEN` (paylaşılmaz, CRM'den bağlanır)
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_BUSINESS_ACCOUNT_ID`

## 4. Platform API — Multi-tenant AI Agent
Deploy sonrası tüm platform endpoint'leri hazır:

| Endpoint | Açıklama |
|----------|----------|
| `GET /api/platform/status` | Platform genel durum |
| `GET /api/tenants` | Tenant listesi |
| `GET /api/tenants/{slug}` | Tenant detay (config dahil) |
| `POST /api/tenants` | Yeni tenant oluştur |
| `PATCH /api/tenants/{slug}/config` | Tenant güncelle (kanal/aksiyon/config) |
| `POST /api/tenant/{slug}/scrape` | URL → AI ile otomatik config çıkar |
| `POST /api/channel/webchat` | Web Chat mesaj gönder/al |
| `GET /api/channel/webchat/session/{id}` | Web Chat geçmişi |
| `GET /api/channel/whatsapp` | WhatsApp webhook verify |
| `POST /api/channel/whatsapp` | WhatsApp inbound mesaj |
| `POST /api/followup/run` | Otomatik follow-up tetikle |
| `POST /api/handoff/notify` | İnsan devralma bildirimi |
| `GET /webchat/widget.js` | Embed widget JS |
| `GET /health` | Servis sağlık |
| `GET /version` | Commit hash + deploy zamanı |

## 5. Web Chat Widget
Siteye ekle:
```html
<script src="https://YOUR-SERVICE.onrender.com/webchat/widget.js?tenant=slug" defer></script>
```

## 6. CRM Bağlantısı (doel-crm.vercel.app)
CRM içinde `/ai-agent` sayfasından:
- Website scrape (AI otomatik öğrenir)
- WhatsApp bağlantısı
- Web Chat widget kodu
- Kanal durumu görüntüleme

## 7. n8n Workflows (opsiyonel)
`workflows/` dizinindeki JSON'ları n8n'e import et:
- `ai-agent-handoff-telegram.json` — insan devralma → Telegram
- `ai-agent-crm-sync.json` — periyodik CRM senkronizasyonu

## 8. Test
Deploy sonrası kontrol et:
- `/health`
- `/api/llm-health`
- `/api/platform/status`
- `/api/channel/webchat` (POST test mesajı)
- Web Chat widget embed

## Blueprint Notu
`render.yaml` web service ile birlikte ücretsiz Render Postgres (`instagram-booking-db`) oluşturur ve `DATABASE_URL` otomatik bağlanır.
