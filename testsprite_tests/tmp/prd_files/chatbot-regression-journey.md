# Chatbot Regression Journeys (Multi-Business)

Yeniden yapılandırılmış Generic Chat Core sisteminin canlıda ve local ortamda çapraz business profilleri arasında izolasyonunu kanıtlayan regresyon test sonuçlarıdır. Hardcode'lar başarıyla silinmiş ve yanıtlar konfigürasyondan çekilmiştir.

## 1. DOEL Digital Agency (Live Production Config)
*Live DB profiliyle Render üzerinden doğrulanmıştır.*

- **User**: CRM işime yarar mı?
- **Bot**: CRM, gelen müşteri taleplerini, randevuları ve takip süreçlerini düzenlemek istiyorsanız işe yarar. Eğer müşteriler karışıyor, geri dönüşler unutuluyor veya randevuları manuel takip ediyorsanız mantıklı olur.
- **User**: Web tasarımla web sitesi aynı şey değil mi?
- **Bot**: Evet, çoğu zaman aynı anlamda kullanılır. Web tasarım, web sitesinin görünüm, yapı ve kullanıcı deneyimi tarafını ifade eder.
- **User**: Otomasyon ne demek?
- **Bot**: Otomasyon, tekrar eden işleri sistemin otomatik yapmasıdır. Örneğin gelen mesajlara yanıt verme, randevu toplama ve müşteri takibini düzenleme gibi süreçleri kolaylaştırır.
- **Status**: PASSED

---

## 2. Beauty Salon (Staging Mock Config)
*Sistem \`app/config/beauty.json\` objesi yüklenmişken denenmiştir.*

- **User**: Hydrafacial ne demek?
- **Bot**: Hydrafacial, cildi temizleme, nemlendirme ve leke bakımı sürecini vakumlu özel başlıklarla bir arada sunan bir cihazlı cilt bakım uygulamasıdır.
- **User**: Lazer epilasyon fiyatı ne?
- **Bot**: Fiyat seçilecek hizmete göre değişiyor. Detayları kısa bir uygulama randevusude netleştirebiliriz.
- **Status**: PASSED

---

## 3. Dental Clinic (Staging Mock Config)
*Sistem \`app/config/dental.json\` objesi yüklenmişken denenmiştir.*

- **User**: İmplant ne demek?
- **Bot**: İmplant, eksik dişin yerine çene kemiğine yerleştirilen yapay diş kökü uygulamasıdır.
- **User**: Muayene ücreti ne kadar?
- **Bot**: Fiyat seçilecek hizmete göre değişiyor. Detayları kısa bir muayene randevusude netleştirebiliriz.
- **User**: Doktorla konuşabilir miyim?
- **Bot**: Sorunuzu doğrudan cevaplayayım; bildiğim kısmı net aktarırım, emin olmadığım yerde de uydurmadan belirtirim.
- **User** *(Booking loop injection)*: Yüksel Yiğit
- **Bot**: Tabii, implant için muayene randevusu planlayabiliriz. Telefon numaranızı yazar mısınız?
- **Status**: PASSED
