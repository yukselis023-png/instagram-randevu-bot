# OpenCode Handoff — Doel System

## Hedef
- IG DM bot + CRM tam çalışır hale getir
- Render (production) backend → local LLM erişimi sağla
- CRM'de gerçek veri gösterimi
- AI servis kataloğu senkronizasyonu

## Repo Konumları
- **Backend:** `C:/Users/oyunc/Desktop/instagram-randevu-bot/`
- **CRM Frontend:** `C:/Users/oyunc/Desktop/doel-crm-recovered/`
- **CRM Live:** `https://doel-crm.vercel.app`
- **Backend Live:** `https://instagram-randevu-bot.onrender.com`
- **GitHub:** `https://github.com/yukselis023-png/instagram-randevu-bot.git`

## Mevcut Durum (2026-05-31 15:00)

### ✅ Çalışan Parçalar
1. **Local Docker bot** — Tam çalışıyor, local LLM erişimi var
   - `docker compose up -d` → booking-api (port 18000), poller, postgres
   - LLM: `http://host.docker.internal:8045/v1` (antigravity_tools.exe proxy)
   - API key: `sk-93ac4612b7b5427d9de03ec1b96e8f26`
   - Model: `gemini-3-flash`

2. **CRM Frontend (Vercel)** — Deploy edildi, API endpoint'leri çalışıyor
   - Fake/demo data kaldırıldı
   - `filterBusinessRecords()` ile test kayıtlar filtreleniyor
   - `Promise.allSettled` ile partial failure handling

3. **Tüm API endpoint'leri** — Render'da 200 OK
   - `/health`, `/api/appointments`, `/api/appointments/calendar`
   - `/api/service-capacity`, `/api/roi-summary`, `/api/customers`
   - `/api/campaigns`, `/api/crm/templates`, `/api/crm/rules`

4. **generic_core.py fixes** — Deploy edildi (commit `c2f7e36`)
   - `1 Haziran` artık `2026-06-01` parse ediyor (eskiden `2026-05-31` idi)
   - Time acceptance detection: `is_time_acceptance_message` regex
   - `last_availability_date` memory anchoring
   - `TAKVİM SORGUSU SONUCU` context injection

### ❌ Ana Sorun: Render → LLM Erişimi
**Problem:** Render (cloud) local LLM'e (`127.0.0.1:8045`) erişemez.

**Denenen Çözümler:**
- Cloudflare Quick Tunnel → Ephemeral, her restart'ta URL değişir
- Groq API → Key geçersiz (biz Groq kullanmıyoruz)
- render.yaml'da hardcoded tunnel URL → Render dashboard env var bunu eziyor

**Son Tunnel URL (ölü olabilir):**
```
https://alumni-definitions-eddie-dale.trycloudflare.com/v1
```

**Kodda Hardcoded Fallback (main.py ~satır 92-106):**
```python
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "").strip().rstrip("/")
_DEAD_TUNNELS = [
    "intent-association-radar-route.trycloudflare.com",
    "scores-unable-texts-fish.trycloudflare.com",
    "afford-fun-thorough-hints.trycloudflare.com",
]
if any(dead in LLM_BASE_URL for dead in _DEAD_TUNNELS):
    LLM_BASE_URL = ""
if not LLM_BASE_URL:
    LLM_BASE_URL = "https://alumni-definitions-eddie-dale.trycloudflare.com/v1"
```

### 🔧 Kalıcı Çözüm Seçenekleri
1. **Google Gemini API Key** — En basit. https://aistudio.google.com/apikey (ücretsiz). Key'i alıp `LLM_API_KEY` env var'a koy.
2. **Named Cloudflare Tunnel** — Sabit URL, Cloudflare hesabı lazım.
3. **Render Dashboard'dan env var güncelle** — `LLM_BASE_URL`'ı çalışan bir tunnel URL'sine çevir.

## render.yaml Kritik Env Vars
```yaml
LLM_BASE_URL: "https://alumni-definitions-eddie-dale.trycloudflare.com/v1"
LLM_MODEL: gemini-3-flash
LLM_FALLBACK_MODEL: gemini-3-flash
LLM_REPLY_MICRO_MODEL: gemini-3-flash
LLM_REPLY_ADVISORY_MODEL: gemini-3-flash
LLM_REPLY_QUALITY_MODEL: gemini-3-flash
LLM_API_KEY: "sk-93ac4612b7b5427d9de03ec1b96e8f26"
LIVE_CRM_ENABLED: "true"
LIVE_CRM_SUPABASE_URL: "https://rnjkilyiqnqiyqhwqdly.supabase.co"
```

**NOT:** Render dashboard'daki env var'lar render.yaml'ı ezer. Dashboard'dan da güncellemek gerekir.

## Dosya Haritası

### Backend (`instagram-randevu-bot/`)
| Dosya | Açıklama |
|---|---|
| `booking-api/app/main.py` | Ana FastAPI app, LLM config, tüm endpoint'ler |
| `booking-api/app/generic_core.py` | DM processing engine, intent detection, booking flow |
| `booking-api/app/actions.py` | Appointment CRUD, Supabase sync |
| `booking-api/app/config/settings.py` | Business settings, working hours |
| `booking-api/app/config/doel.json` | Service catalog, pricing |
| `instagram-poller/app/main.py` | IG DM poller, Meta API integration |
| `render.yaml` | Render deploy config, env vars |
| `docker-compose.yml` | Local Docker setup |

### CRM (`doel-crm-recovered/`)
| Dosya | Açıklama |
|---|---|
| `src/main.jsx` | Tüm CRM UI (tek dosya, ~7000 satır) |

## Son Git Durumu
```
dd07d57 fix: update LLM tunnel to working URL       ← EN SON
e6c5544 fix: update LLM tunnel URL
a56ab3a fix: hardcode working LLM tunnel, remove dead overrides
e49b4d8 fix: use Cloudflare tunnel for Render LLM access
882e086 fix: remove dead Cloudflare tunnel from LLM_BASE_URL
1c6181f feat: sync live CRM services into AI prompt context
c2f7e36 fix: make IG booking slot acceptance reliable
```

CRM deploy (Vercel):
```
af596aa fix: remove demo data, improve API errors, filter test records
```

## Kalan İşler

### Öncelik 1: LLM Erişimi (Render)
- [ ] Gemini API key al veya kalıcı tunnel kur
- [ ] Render dashboard env var'larını güncelle
- [ ] Smoke test: full booking flow (service → name+phone → date → accept → appointment created)

### Öncelik 2: Smoke Test
```bash
python - <<'PY'
import requests, uuid, time
base='https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
sender='smoke-'+str(int(time.time()))
msgs=[
    ('web tasarim istiyorum','svc'),
    ('adi Ahmet Vefik tel 05539088766','name'),
    ('1 Haziranda bos saat var mi?','date'),
    ('12 ayarlayabilirseniz kabul ediyormus','accept'),
]
for m,l in msgs:
    d=requests.post(base,json={'sender_id':sender,'instagram_username':'t','message_text':m,'trace_id':str(uuid.uuid4()),'raw_event':{'id':str(uuid.uuid4()),'created_at':time.time()}},timeout=120).json()
    print(f'[{l}] reply={d.get("reply_text","")[:120]} created={d.get("appointment_created")} date={d.get("normalized",{}).get("requested_date")} time={d.get("normalized",{}).get("requested_time")}')
PY
```

**Beklenen Sonuçlar:**
- `[svc]` → Hizmet ilgisi algılandı, davet mesajı
- `[name]` → "Ahmet Vefik" kaydedildi, randevu sorusu
- `[date]` → 1 Haziran müsaitlik listesi (10:00, 11:00, 12:00...)
- `[accept]` → `appointment_created=True`, `Ön görüşme kaydınız oluşturuldu: 01.06.2026 saat 12:00`

### Öncelik 3: CRM Real Data
- [ ] CRM pages'da Supabase'den gerçek veri çekildiğini doğrula
- [ ] ₺9,997 test verisini Supabase'den sil
- [ ] Takip & Kazanç "Failed to fetch" hatasını debug et (CORS/env issue)

### Öncelik 4: AI Katalog Bridge
- [ ] CRM'deki `services` tablosundaki hizmetler AI prompt'a inject ediliyor (`build_ai_first_service_context`)
- [ ] `fetch_live_crm_services_for_ai()` fonksiyonu çalışır durumda (main.py'de)

## Local Docker Komutları
```bash
cd C:/Users/oyunc/Desktop/instagram-randevu-bot
docker compose up -d                    # Tüm servisleri başlat
docker logs ig-randevu-api --tail 50    # Booking API logları
docker logs ig-randevu-poller --tail 50 # Poller logları
docker exec ig-randevu-api curl http://localhost:8000/health  # Health check
```

## LLM Proxy (Local)
- Process: `antigravity_tools.exe` (PID değişken)
- Port: `8045`
- Base URL: `http://127.0.0.1:8045/v1`
- API Key: `sk-93ac4612b7b5427d9de03ec1b96e8f26`
- Model: `gemini-3-flash`
- Upstream: Google Gemini API (OAuth2 ile)
