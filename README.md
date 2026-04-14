# Instagram n8n Randevu Botu

Bu klasör, Instagram DM üzerinden randevu toplamak için hazırlanmış bir **Docker + n8n + FastAPI + PostgreSQL** kurulumudur.

## Bileşenler
- **n8n**: webhook ve akış orkestrasyonu
- **booking-api**: randevu mantığı, konuşma durumu, LLM tabanlı çıkarım
- **PostgreSQL**: konuşmalar, randevular, mesaj logları
- **cloudflared**: test için geçici public webhook URL

## Yerel erişim
- n8n: [http://localhost:5678](http://localhost:5678)
- booking-api health: [http://localhost:18000/health](http://localhost:18000/health)
- booking-api appointments: [http://localhost:18000/api/appointments](http://localhost:18000/api/appointments)

## n8n basic auth
- Kullanıcı adı: `doel`
- Şifre: `.env` dosyasında `N8N_BASIC_AUTH_PASSWORD`

## n8n owner hesabı
- E-posta: `doel@local.test`
- Şifre: `.env` dosyasındaki `N8N_BASIC_AUTH_PASSWORD` ile aynı ayarlandı

## LLM notu
Yerel OpenAI-uyumlu endpointte stabil çalışan model sırası kullanılmaktadır:
- birincil: `gemini-3-flash`
- fallback: `gemini-2.5-flash-lite`

Daha önce denenen `gemini-3.1-flash-lite` gateway tarafında kararsız / hatalı davrandığı için aktif yol olarak bırakılmadı.

## Instagram token durumu
Gerçek Instagram business login akışı tamamlandı:
- app oluşturuldu
- tester rolü verildi
- davet kabul edildi
- access token üretildi
- long-lived token alındı
- webhook subscription açıldı

`.env` içinde şu alanlar artık dolu:
- `IG_ACCESS_TOKEN`
- `IG_BUSINESS_USER_ID`

## Başlatma
PowerShell:

```powershell
cd C:\Users\oyunc\Desktop\instagram-randevu-bot
.\scripts\start-stack.ps1
```

## Workflow import
İlk kurulumdan sonra veya tekrar gerektiğinde:

```powershell
cd C:\Users\oyunc\Desktop\instagram-randevu-bot
.\scripts\import-workflows.ps1
```

## Lokal test
Bu script artık n8n içindeki AI router webhook'unu test eder:

```powershell
cd C:\Users\oyunc\Desktop\instagram-randevu-bot
.\scripts\test-local-message.ps1
```

## Public webhook URL alma
Şu an çalışan quick tunnel URL:
- `https://belong-monetary-realtors-extras.trycloudflare.com`

Cloudflared quick tunnel URL'sini tekrar görmek için:

```powershell
cd C:\Users\oyunc\Desktop\instagram-randevu-bot
.\scripts\get-tunnel-url.ps1
```

Webhook path:
- Verify + events callback: `<TUNNEL_URL>/webhook/instagram/randevu`
- Şu anki tam callback: `https://belong-monetary-realtors-extras.trycloudflare.com/webhook/instagram/randevu`

## Aktif çalışan akış
Sistem şu anda aktif olarak **Instagram private API poller + n8n AI router** ile çalışır:
- `instagram-poller` servisi DM'leri poll eder
- mesajları `n8n` içindeki `Instagram Message Bot` webhook'una gönderir
- `n8n` mesajı `booking-api` servisine iletir
- `booking-api` yerel LLM endpointini kullanarak niyet çıkarımı ve cevap iyileştirmesi yapar
- oluşan yanıt tekrar poller üzerinden Instagram DM olarak gönderilir

Bu yol, Meta app review / live mode beklemeden pratikte çalışır ve AI artık n8n otomasyon zincirinin içindedir.

## Meta tarafı notu
Meta app / webhook kurulumu yine de klasörde korunuyor. Ancak canlı teslimatta Meta kısıtları sorun çıkardığı için şu an ana taşıyıcı yol `instagram-poller -> n8n -> booking-api` zinciridir.

İleride istenirse yeniden resmi webhook yoluna dönülebilir.

Stack'i yeniden başlatmak için:

```powershell
docker compose up -d --build
```

## Varsayılan randevu ayarları
`.env` içinden değiştirilebilir:
- çalışma saatleri: `10:00 - 19:00`
- slot süresi: `60 dakika`
- ileri tarih sınırı: `30 gün`

## Kayıtlar
- Konuşmalar: `conversations`
- Randevular: `appointments`
- Mesaj logları: `message_logs`

## Dosyalar
- Compose: [docker-compose.yml](C:\Users\oyunc\Desktop\instagram-randevu-bot\docker-compose.yml)
- Ortam değişkenleri: [.env](C:\Users\oyunc\Desktop\instagram-randevu-bot\.env)
- Verify workflow: [workflows/instagram-webhook-verify.json](C:\Users\oyunc\Desktop\instagram-randevu-bot\workflows\instagram-webhook-verify.json)
- Message workflow: [workflows/instagram-message-bot.json](C:\Users\oyunc\Desktop\instagram-randevu-bot\workflows\instagram-message-bot.json)
