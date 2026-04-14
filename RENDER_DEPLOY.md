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
- `DATABASE_URL`
- `LLM_BASE_URL`
- `LLM_API_KEY`
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
## 4. Frontend bağlantısı
Vercel tarafında:
`VITE_CRM_API_BASE_URL=https://YOUR-RENDER-SERVICE.onrender.com`
## 5. Test
Deploy sonrası kontrol et:
- `/health`
- `/api/roi-summary`
- `/api/customers/ops/no-show`
- `/api/customers/ops/upcoming-automations`
