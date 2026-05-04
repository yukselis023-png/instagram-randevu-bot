import json, os

os.makedirs("app/config", exist_ok=True)

doel = {
    "business_name": "DOEL Digital",
    "business_type": "Dijital Ajans",
    "business_tagline": "Dijital büyüme ortağınız",
    "human_contact_name": "Berkay",
    "appointment_service_labels": ["danışmanlık", "görüşme", "toplantı", "ön görüşme"],
    "tone": "professionally helpful, direct, reassuring",
    "booking_mode": "ön görüşme",
    "unavailable_services": ["saç kesimi", "lazer", "cilt bakımı", "dövme", "gayrimenkul", "emlak", "araç satışı", "doktor muayenesi"],
    "fallback_handoff_text": "İsterseniz mesajınızı ekibe iletebilirim.",
    "service_catalog": [
        {
            "slug": "web-tasarim",
            "display": "Web Tasarım",
            "keywords": ["web tasarım", "web tasarim", "web sitesi", "website", "kurumsal site", "landing page", "açılış sayfası", "site yenileme"],
            "price": "12.900 TL",
            "price_note": "tek seferlik paket fiyatı",
            "summary": "Google uyumlu, tüm cihazlara tam uyumlu, WhatsApp butonlu, 1 yıl altyapı garantili kurumsal web tasarım çözümü.",
            "clarification": "Web tasarım, web sitesinin görünüm, yapı ve kullanıcı deneyimi tarafını ifade eder. Genellikle web sitesi yapmak, kurmak anlamında kullanılır.",
            "fit_description": "İnternette profesyonel bir vitrin oluşturmak, müşterilerinize güven vermek ve hizmetlerinizi detaylıca sunmak istiyorsanız tam size göredir."
        },
        {
            "slug": "otomasyon-ai",
            "display": "Otomasyon & Yapay Zeka Çözümleri",
            "keywords": ["otomasyon", "yapay zeka", "chatbot", "n8n", "ai", "dm otomasyonu", "instagram bot", "randevu botu", "crm"],
            "price": "5.000 TL",
            "price_note": "ilk 3 ay indirimli aylık hizmet bedeli",
            "summary": "Müşteri mesajlarına 7/24 yanıt, randevuları otomatik ayarlama, teklif ve fatura otomasyonu içerir.",
            "clarification": "Otomasyon, gelen mesajları anında yanıtlayan, tekrar eden işleri insansız çözen ve müşterileri kaydeden yazılım altyapısıdır.",
            "fit_description": "Gelen mesajlara yetişemiyorsanız, randevu alımları karışıyorsa veya müşterilere geç dönüş yapmaktan müşteri kaybediyorsanız işe yarar. CRM, gelen müşteri taleplerini, randevuları ve takip süreçlerini düzenlemek istiyorsanız mantıklıdır."
        },
        {
            "slug": "performans-pazarlama",
            "display": "Performans Pazarlama",
            "keywords": ["performans pazarlama", "reklam", "meta reklam", "tiktok reklam", "instagram reklam", "meta ads", "müşteri kazanmak"],
            "price": "7.500 TL",
            "price_note": "aylık danışmanlık bedeli, reklam bütçesi hariç",
            "summary": "Meta ve TikTok reklam yönetimi, hedef kitle ve rakip analizi, kreatif reklam tasarımları sunar.",
            "clarification": "Performans pazarlama, doğrudan satış veya randevu getirmeye odaklı, ölçülebilir dijital reklam kampanyaları yönetimidir.",
            "fit_description": "Yeni müşteriler bulmak ve satışlarınızı ölçeklemek ana hedefinizse reklam en doğru yoldur."
        },
        {
            "slug": "sosyal-medya-yonetimi",
            "display": "Sosyal Medya Yönetimi",
            "keywords": ["sosyal medya", "içerik yönetimi", "sayfa yönetimi", "içerik üretimi", "reels yönetimi"],
            "price": "Özel teklif",
            "price_note": "marka ihtiyacına göre belirlenir",
            "summary": "Topluluk inşası, kriz yönetimi, içerik planlama ve sürdürülebilir sosyal medya yönetimi sunar.",
            "clarification": "Sosyal medya yönetimi, hesabınızın düzenli içeriklerle profesyonel görünmesi, takipçilerle etkileşim ve güven oluşturması sürecidir.",
            "fit_description": "Sosyal medya yönetimi, markanızın daha profesyonel görünmesi, düzenli içerik paylaşması ve güven oluşturması için işe yarar. Instagram’da daha görünür olmak ve hesabı düzenli yönetmek istiyorsanız mantıklı olur; direkt müşteri kazanımı hedefleniyorsa reklamla birlikte düşünülmeli."
        }
    ]
}

beauty = {
    "business_name": "Güzellik Merkezi",
    "business_type": "Güzellik Salonu",
    "business_tagline": "Kendinizi özel hissedin",
    "human_contact_name": "yetkili",
    "appointment_service_labels": ["seans", "randevu", "uygulama randevusu"],
    "tone": "warm, welcoming, professional, assuring",
    "booking_mode": "uygulama randevusu",
    "unavailable_services": ["web tasarım", "reklam", "saç kesimi", "ameliyat", "dövme", "gayrimenkul", "otomasyon", "crm"],
    "fallback_handoff_text": "İsterseniz mesajınızı uzman ekibimize iletebilirim.",
    "service_catalog": [
        {
            "slug": "hydrafacial",
            "display": "Hydrafacial",
            "keywords": ["hydrafacial", "cilt bakımı", "derin temizlik", "nemlendirme", "bakım"],
            "price": "1.500 TL",
            "price_note": "tek seans fiyatı",
            "summary": "Cildi derinlemesine temizler, nemlendirir ve besler. Acısız ve konforlu bir işlemdir.",
            "clarification": "Hydrafacial, cildi temizleme, nemlendirme ve leke bakımı sürecini vakumlu özel başlıklarla bir arada sunan bir cihazlı cilt bakım uygulamasıdır.",
            "fit_description": "Cilt bakımı, cildinizde temizlik, nem ve canlılık hedefliyorsanız uygun olabilir. Cilt tipiniz ve beklentiniz uygulama seçiminde önemli olur."
        },
        {
            "slug": "lazer-epilasyon",
            "display": "Buz Lazer Epilasyon",
            "keywords": ["lazer", "epilasyon", "lazer epilasyon", "buz lazer", "tüy alma"],
            "price": "3.500 TL",
            "price_note": "tüm vücut 6 seans paket",
            "summary": "Acısız buz başlık teknolojisiyle tüm cilt tiplerinde etkili, kalıcı tüy dökme işlemi.",
            "clarification": "Buz lazer epilasyon, kıl köklerini kalıcı olarak zayıflatan ve dökülmesini sağlayan yeni nesil acısız ve konforlu bir medikal yöntemdir.",
            "fit_description": "Tüylerinizden kalıcı olarak kurtulmak ve pürüzsüz bir cilt elde etmek istiyorsanız %90'a varan kalıcı çözüm sunar."
        }
    ]
}

dental = {
    "business_name": "Estetik Diş Kliniği",
    "business_type": "Diş Kliniği",
    "business_tagline": "Sağlıklı gülüşler",
    "human_contact_name": "doktorumuz",
    "appointment_service_labels": ["muayene", "randevu", "kontrol"],
    "tone": "professional, clinical, reassuring, gentle",
    "booking_mode": "muayene randevusu",
    "unavailable_services": ["web tasarım", "reklam", "cilt bakımı", "lazer epilasyon", "genel cerrahi", "kardiyoloji", "otomasyon", "sosyal medya"],
    "fallback_handoff_text": "Dilerseniz bilgilerinizi alayım, klinik asistanımız sizi arasın.",
    "service_catalog": [
        {
            "slug": "implant",
            "display": "İmplant Tedavisi",
            "keywords": ["implant", "vidalı diş", "vidali dis", "yapay diş", "eksik diş", "çene kemiği"],
            "price": "Klinik muayenede belirlenir",
            "price_note": "vaka durumuna ve kullanılacak markaya göre değişir",
            "summary": "Eksik dişlerin fonksiyon ve estetiğini geri kazandıran, çene kemiğine yerleştirilen titanyum destekli yapay diş kökü tedavisi.",
            "clarification": "İmplant, eksik dişin yerine çene kemiğine yerleştirilen yapay diş kökü uygulamasıdır.",
            "fit_description": "Çekilmiş dişleriniz ve ağzınızda boşluklar varsa, çiğneme fonksiyonunuzu doğal dişe en yakın hisle geri kazanmak için ağız ve çene yapınızın incelenmesi gerekir."
        },
        {
            "slug": "dis-beyazlatma",
            "display": "Diş Beyazlatma (Bleaching)",
            "keywords": ["diş beyazlatma", "bleaching", "dis beyazlatma", "sararan diş", "diş estetiği", "gülüş estetiği"],
            "price": "3.000 TL",
            "price_note": "tek seans ofis tipi beyazlatma",
            "summary": "Lazer ışık desteği ve beyazlatıcı jellerle diş renginin 2-4 ton açıldığı profesyonel klinik beyazlatma işlemi.",
            "clarification": "Diş beyazlatma, diş yüzeyindeki gözeneklere işlemiş olan renklenmeleri diş hekimi kontrolünde açma ve dişi kendi renginden birkaç ton daha beyazlatma işlemidir.",
            "fit_description": "Diş beyazlatma, diş renginizden memnun değilseniz ve daha estetik bir görünüm istiyorsanız uygun olabilir. Uygunluk için diş hekiminin kısa değerlendirmesi gerekir."
        }
    ]
}

with open("app/config/doel.json", "w", encoding="utf-8") as f: json.dump(doel, f, indent=4, ensure_ascii=False)
with open("app/config/beauty.json", "w", encoding="utf-8") as f: json.dump(beauty, f, indent=4, ensure_ascii=False)
with open("app/config/dental.json", "w", encoding="utf-8") as f: json.dump(dental, f, indent=4, ensure_ascii=False)

settings_py = """import json
import os

def load_business_profile():
    profile = os.getenv("BUSINESS_PROFILE", "doel")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{profile}.json")
    if not os.path.exists(file_path):
        file_path = os.path.join(current_dir, "doel.json")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print("Config load error:", e)
        return {"business_name": "Generic Business", "service_catalog": []}

CONFIG = load_business_profile()

def get_config():
    return CONFIG
"""
with open("app/config/settings.py", "w", encoding="utf-8") as f: f.write(settings_py)
