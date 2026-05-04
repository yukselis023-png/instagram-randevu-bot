import requests
import time
import uuid

TARGET_URL = "https://instagram-randevu-bot.onrender.com/api/process-instagram-message"
SENDER_ID = f"live_test_{uuid.uuid4().hex[:6]}"

messages = [
    "Merhaba, iyi günler",
    "DOEL tam olarak kimdir ve şirket geçmişiniz nedir?",
    "Peki sen gerçek bir insan mısın yoksa bot mu?",
    "Benim e-ticaret işim var. CRM bana uygun olur mu?",
    "Asıl amacım daha fazla satış yapmak. Sosyal medya yönetimi bana uyar mı?",
    "Maliyetleri merak ediyorum, mesela web tasarımı kaç para?",
    "Fiyatlar biraz yüksekmiş, indirim veya vade farksız taksit imkanı var mı?",
    "Kredi kartı kabul ediyor musunuz?",
    "Tamam, peki paketlerinize saç kesimi dahil mi?",
    "Anladım, o zaman bana Sosyal Medya Yönetimi için devam edelim.",
    "05551234567",
    "Adım Ahmet Yılmaz",
    "Önümüzdeki cuma saat 14:00 uygun",
    "Teşekkürler. Rezervasyon haricinde ofis adresiniz nedir?",
    "Berkay diye birini duydum, doğrudan onunla görüşmek istiyorum, mümkün mü?"
]

print(f"--- CANLI TEST BAŞLIYOR (SENDER_ID: {SENDER_ID}) ---")
for idx, text in enumerate(messages, 1):
    print(f"\n[USER] {text}")
    try:
        response = requests.post(
            TARGET_URL,
            json={"sender_id": SENDER_ID, "message_text": text},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            print(f"[BOT] {data.get('reply_text', '--- no final_reply field ---')}")
        else:
            print(f"[ERROR] HTTP {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] BAĞLANTI HATASI: {e}")
    
    time.sleep(1.5)

print("\n--- TEST TAMAMLANDI ---")