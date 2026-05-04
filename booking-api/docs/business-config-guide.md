# Business Config Kurulum Rehberi

Bu rehber, Generic Chat Core mimarisinin yeni bir işletmeye nasıl kurulacağını açıklamaktadır. Sistem %100 \"business-agnostic\" (sektörden bağımsız) olarak tasarlanmıştır. Davranış, ton, hizmetler ve özel stringlerin tamamı \`.json\` profil dosyalarıyla beslenir.

## 1. Konfigürasyon Dosyası Oluşturma
\`app/config/\` dizini altına işletmenizin adıyla bir (örn. \`benim-isletmem.json\`) dosyası oluşturun. Şema beklentisi aşağıdaki gibidir:

```json
{
    "business_name": "İşletme Adı",
    "business_type": "Kategori (örn. Ajans, Klinik)",
    "business_tagline": "Kısaca Ne Yapar?",
    "human_contact_name": "Yetkili (örn. Berkay, Klinik Asistanı, Doktorumuz)",
    "tone": "Örn. ikna edici ve profesyonel",
    "booking_mode": "Randevu türü (örn. ön görüşme, muayene randevusu)",
    "unavailable_services": ["Yalın array şeklinde verilmeyen hizmetler"],
    "appointment_service_labels": ["randevu", "ön görüşme", "toplantı", "seans"],
    "fallback_handoff_text": "Sistemin bilmediği bir durumda insana aktarım mesajı",
    "service_catalog": [
        {
            "slug": "hizmet-slug-1",
            "display": "Ekranda Görünecek Tam İsim",
            "keywords": ["analiz", "tetikleyici", "farklı", "kelimeler"],
            "price": "Fiyat Değeri",
            "price_note": "Fiyata Dair Dipnot",
            "delivery_time": "Ne kadar sürer",
            "summary": "Özet tanıtım cümlesi",
            "clarification": "Bu hizmet nedir tam açıklaması",
            "fit_description": "Bu hizmet benim işime yarar mı diyen kullanıcıya uygunluk açıklaması"
        }
    ]
}
```

## 2. Aktifleştirme
Bot sisteminin bu dosyayı baz alması için çevre değişkenlerinde (\`Environment Variable\`) aşağıdaki konfigürasyon ayarlanmalıdır:
\`BUSINESS_PROFILE=benim-isletmem\`

## 3. Router Modülleri ve Davranışları
- **Term Clarification (X Nedir?)**: Algoritma \`keywords\` dizisinde geçerli terimi arar ve saptarsa \`.clarification\` değerini döndürür.
- **Service Fit (Bana Uygun Mu?)**: Aynı şekilde saptanan hizmetin \`.fit_description\` değerini tavsiye olarak sunar.
- **Fiyat Sorgusu**: Hizmet tanınırsa \`.price\` ve \`.price_note\` sunulur. Ancak eksikse/tanınmazsa genel fiyat itirazını engellemek için sistem \`booking_mode\` konfigürasyonunu okuyarak "Fiyat seçilecek hizmete göre değişiyor. Detayları kısa bir {booking_mode}de netleştirebiliriz" yanıtını türetir.
