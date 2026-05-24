# Tenant Onboarding — Doel AI Agent

## Yeni Müşteri Kurulumu (15 dk)

### Adım 1: CRM Hesabı
1. **https://doel-crm.vercel.app/register** — işletme hesabı oluştur
2. Email doğrula
3. Onboarding adımlarını tamamla (çalışma saatleri, hizmetler, personel)

### Adım 2: AI Agent'i Aktifleştir
1. CRM sol menüden **AI Agent** sayfasına git
2. Website URL gir → **"Website'den AI Agent oluştur"** butonuna tıkla
   - Sistem otomatik: işletme adı, hizmetler, fiyatlar, tonlamayı çıkarır
   - Elle düzeltme gerekiyorsa `/ai-agent` sayfasından güncelle

### Adım 3: WhatsApp Bağlantısı
1. Meta Business hesabından WhatsApp Cloud API bilgilerini al
2. AI Agent sayfasında **WhatsApp bağlantısı** formuna gir:
   - Phone Number ID
   - Business Account ID
   - Access Token (kalıcı token)
3. Webhook doğrulamasını Meta'ya bildir:
   ```
   Callback URL: https://instagram-randevu-bot.onrender.com/api/channel/whatsapp
   Verify Token: doelai_verify
   ```

### Adım 4: Instagram DM
- Instagram DM zaten aktif (mevcut bot altyapısı)
- n8n webhook: `https://YOUR-INSTANCE.onrender.com/webhook/instagram/ai-router`
- Webhook verify token: `.env` içinde `META_VERIFY_TOKEN`

### Adım 5: Web Chat
Müşterinin sitesine ekle:
```html
<script src="https://instagram-randevu-bot.onrender.com/webchat/widget.js?tenant=MUSTERI_SLUG" defer></script>
```

### Adım 6: Telegram Handoff (opsiyonel)
1. @BotFather ile bot oluştur → token al
2. `.env`'e ekle: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_HANDOFF_CHAT_ID`
3. n8n workflow import: `workflows/ai-agent-handoff-telegram.json`

### Adım 7: Takip
- **Randevular**: CRM takvim/appointments ekranı
- **Müşteriler**: CRM customers
- **AI Agent kanal durumu**: CRM AI Agent sayfası
- **Otomatik takip**: Sistem 24 saat sonra cevap vermeyen lead'lere otomatik mesaj gönderir

## AI Agent Davranışı
- **Kanallar**: Instagram DM + WhatsApp + Web Chat
- **Aksiyonlar**: Randevu al / Ara / Ziyaret et / Form doldur
- **Lead scoring**: 0-100 (hot/warm/cold)
- **Human handoff**: Müşteri "insan/operatör" isterse → Telegram bildirimi + CRM uyarı
- **Admin panel yok** → Tüm yönetim CRM'de
- **Sesli arama yok**

## API Anahtarları (white-label partner)
Platform API: `https://instagram-randevu-bot.onrender.com`
Tenant bazlı endpoint'ler: `/api/tenants/{slug}/...`
