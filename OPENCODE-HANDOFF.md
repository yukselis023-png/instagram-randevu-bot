# OpenCode Handoff — Doel IG CRM System
> Session: 260531-ready-swamp | Tarih: 2026-05-31 21:30 +03

## Hedef
Instagram DM üzerinden randevu toplayan, LLM destekli bot + CRM dashboard sistemi.
Kritik hedef: **Render'da çalışan kalıcı LLM erişimi** → full booking flow çalışır hale gelmeli.

## Repo Konumları
| Repo | Path | Live URL |
|---|---|---|
| Backend | `C:/Users/oyunc/Desktop/instagram-randevu-bot/` | https://instagram-randevu-bot.onrender.com |
| CRM Frontend | `C:/Users/oyunc/Desktop/doel-crm-recovered/` | https://doel-crm.vercel.app |
| GitHub | — | https://github.com/yukselis023-png/instagram-randevu-bot.git |

## Modül Haritası (DAG)

```
MODÜL GRAFİĞİ (bağımlılıklar ok yönünde)
═══════════════════════════════════════════

    ┌──────────────┐
    │ M0: LLM      │  ← PARALEL: Tunnel veya API key
    │   Erişimi     │
    └──────┬───────┘
           ▼
    ┌──────────────┐
    │ M1: Booking  │  ← BAĞIMLI: M0'a (LLM endpoint)
    │   API Core   │
    └──────┬───────┘
           ▼
    ┌──────┴───────┐
    ▼              ▼
┌────────┐  ┌──────────┐  ← PARALEL: M1'den sonra
│ M2:    │  │ M3: AI   │
│ Poller │  │ Katalog  │
└───┬────┘  │ Bridge   │
    ▼       └────┬─────┘
    ┌────────────┘
    ▼
┌──────────────┐
│ M4: CRM      │  ← BAĞIMLI: M1 + M3 API'lerine
│   Frontend   │
└──────┬───────┘
       ▼
┌──────────────┐
│ M5: Smoke    │  ← BAĞIMLI: Hepsi
│   Test       │
└──────────────┘
```

## Modül Detayları

### M0: LLM Erişimi (Render → Cloud) — P0/KRİTİK — ✅ ÇALIŞIYOR
**Durum:** `gemini-3-flash` modeli ile çalışıyor. Proxy (antigravity_tools.exe) üzerinden.
**Aktif Tunnel:** `https://shipping-jump-cold-webmasters.trycloudflare.com/v1`
**Dashboard Env'ler:** Render Dashboard'dan elle güncellendi (LLM_BASE_URL, LLM_MODEL, LLM_FALLBACK_MODEL)
**Not:** `gemini-3.1-flash-lite` proxy'de safety_settings hatası veriyor → bypass için `gemini-3-flash` kullanılıyor.

### M1: Booking API Core — P0 — ✅ Çalışıyor (Local + Render)
**Dosyalar:**
| Dosya | Satır | Görev |
|---|---|---|
| `booking-api/app/main.py` | ~500 | FastAPI routes, LLM config, endpoint'ler |
| `booking-api/app/generic_core.py` | ~1200 | DM processing, intent detection, booking flow |
| `booking-api/app/actions.py` | ~300 | Appointment CRUD, Supabase sync |
| `booking-api/app/pipeline_wrapper.py` | ~100 | Pipeline wrapper |
| `booking-api/app/analytics.py` | ~200 | Analytics/scoring |
| `booking-api/app/scoring.py` | ~150 | Scoring logic |
| `booking-api/app/followup.py` | ~200 | Follow-up reminders |
| `booking-api/app/handoff.py` | ~100 | Handoff to human |
| `booking-api/app/tenant.py` | ~100 | Multi-tenant |
| `booking-api/app/whatsapp.py` | ~100 | WhatsApp integration |
| `booking-api/app/webchat_handler.py` | ~150 | Webchat processing |
| `booking-api/app/scraper.py` | ~100 | Web scraping |
| `booking-api/app/config/settings.py` | ~200 | Business settings, working hours |
| `booking-api/app/config/doel.json` | — | Service catalog, pricing |

**API Endpoint'leri (tümü Render'da 200 OK):**
- `/health`, `/api/appointments`, `/api/appointments/calendar`
- `/api/service-capacity`, `/api/roi-summary`, `/api/customers`
- `/api/campaigns`, `/api/crm/templates`, `/api/crm/rules`

### M2: Instagram Poller — P0 — ✅ Çalışıyor
**Dosyalar:** `instagram-poller/app/main.py`
**Bağımlılık:** M1 (booking-api çalışır olmalı)
**DM yol:** poller → n8n webhook → booking-api → LLM → poller → IG DM

### M3: AI Katalog Bridge — P1 — ✅ Çalışıyor
**Durum:** CRM services tablosu canlı, 10 servis var. fetch_live_crm_services_for_ai çalışıyor. Servisler AI prompt'a doğru inject ediliyor.
**Dosyalar:** `booking-api/app/main.py` (fetch_live_crm_services_for_ai), `booking-api/app/config/doel.json`
**CRIM Services: Web Tasarim(12900TL), Otomasyon(5000TL), Performans Pazarlama(7500TL), Sosyal Medya Yonetimi, Marka Stratejisi, Kreatif Prodiksiyon, REKLAM YONETIMI(7500TL)

### M4: CRM Frontend — P1 — ✅ Live
**Dosya:** `doel-crm-recovered/src/main.jsx` (~7000 satır, tek dosya)
**Canlı URL:** https://doel-crm.vercel.app
**Kalan:**
- [ ] Supabase'den gerçek veri gösterimi doğrulanacak
- [ ] Takip & Kazanç "Failed to fetch" hatası (CORS/env)
- [ ] Bot Performans Grafiği referansa uygun olmalı (koyu arka plan, area/line)

### M5: Smoke Test (E2E) — P2 — ✅ Tamamlandi
**Sonuç:** `final_reply_source=llm_raw`, `llm_model_used=gemini-3-flash` — Web tasarim talebi algilandi.

**E2E Result: 9/9 test pass**
- Happy Path (Web Tasarim): PASS — Appt #57 (Ahmet Vefik) created 2026-06-01 12:00
- Pricing: PASS — info_ai_price_verified pipeline
- Mobile App: PASS — LLM correctly says not in catalog
- Handoff: PASS
- Duplicate: PASS
- Cancellation: PASS
- Greeting: PASS
- SEO: PASS
- Topic Switch: PASS
- Otomasyon & AI service: PASS
- Performans Pazarlama: PASS
- Sosyal Medya Yonetimi: PASS

**CRM Real Data: 15 appts, 19 customers, 10 services in Supabase**
**ALL SYNCING CORRECTLY FROM BOOKING API**

**Bug Fix: Duplicate Task Creation (commit 53678aa)**
- Fixed live_crm_ensure_task_for_conversation dedup
- BEFORE: matched by title+due_date → duplicates on reschedule
- AFTER: matched by title+completed=false → PATCH updates due_date
- Verified: Dedup Test customer has exactly 1 open task

## Çalışma Modeli
```
PHASE AKIŞI:
Build → Review → Fix(max 5 iter) → Merge → E2E

PARALEL KOŞULU:
- M0 bağımsız, hemen başlar
- M2 ve M3 birbirinden bağımsız ama ikisi de M1'e bağlı
- M4 M1+M3'e bağlı
- M5 hepsinin üstünde

AGENT KURALLARI:
- Her modül = 1 agent(), kendi worktree'sinde
- Model: claude-opus-4-8, 1M token bağlam
- Her agent sadece kendi modülü + ilgili arayüz dosyalarını okur
- budget.remaining() < 50.000 → yeni agent spawn etme, mevcut işi bitir
```

## Mevcut Durum Özeti (2026-05-31 17:30)
| Parça | Durum |
|---|---|
| Local Docker bot | ✅ Tam çalışıyor |
| CRM Frontend (Vercel) | ✅ Deploy edildi |
| API Endpoint'leri | ✅ 200 OK |
| generic_core.py fixes | ✅ Deploy (85b2dc5) |
| Render → LLM (gemini-3-flash) | ✅ Çalışıyor |
| Smoke Test (LLM Yanıtı) | ✅ Çalışıyor |
| E2E flow (name→date→accept) | ✅ Tamam (Appt #57) |
| CRM Real Data (15 appt, 19 cust) | ✅ Sync Calisiyor |
| AI Catalog Bridge | ✅ 10 servis |
| Task Duplicate Bug | ✅ FIXED (53678aa) |

## Son Git Durumu
```
53678aa fix(crm): prevent duplicate tasks in live_crm_ensure_task  ← EN SON (HEAD)
85b2dc5 chore: use gemini-3-flash model consistently
770ba30 fix: force correct LLM tunnel URL, ignore stale dashboard env var
e02d83a fix: update LLM tunnel URL, switch to gemini-3.1-flash-lite
dd07d57 fix: update LLM tunnel to working URL
```

## Kritik Env Vars (render.yaml + Dashboard)
```yaml
LLM_BASE_URL: "https://shipping-jump-cold-webmasters.trycloudflare.com/v1"  # AKTİF
LLM_MODEL: gemini-3-flash
LLM_FALLBACK_MODEL: gemini-3-flash
LLM_API_KEY: "sk-93ac4612b7b5427d9de03ec1b96e8f26"
LIVE_CRM_ENABLED: "true"
LIVE_CRM_SUPABASE_URL: "https://rnjkilyiqnqiyqhwqdly.supabase.co"
```

## Render Chrome Session
- Service ID: `srv-d7f6l8favr4c73927gb0`
- Deploy sayfası: `/web/srv-d7f6l8favr4c73927gb0/deploys`
- Env sayfası: `/web/srv-d7f6l8favr4c73927gb0/env`

## Local Docker Komutları
```bash
cd C:/Users/oyunc/Desktop/instagram-randevu-bot
docker compose up -d                    # Başlat
docker logs ig-randevu-api --tail 50    # API logları
docker logs ig-randevu-poller --tail 50 # Poller logları
docker exec ig-randevu-api curl http://localhost:8000/health
```

## LLM Proxy (Local)
- Process: `antigravity_tools.exe` → Port 8045
- Base URL: `http://127.0.0.1:8045/v1`
- API Key: `sk-93ac4612b7b5427d9de03ec1b96e8f26`
- Model: `gemini-3-flash`
