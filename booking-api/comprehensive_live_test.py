import requests
import uuid
import time
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://instagram-randevu-bot.onrender.com"
ENDPOINT = "/api/process-instagram-message"

SCENARIOS = {
    "senaryo_a_hizli_randevu": [
        "Merhaba, hemen bir ön görüşme ayarlamak istiyorum.",
        "Performans pazarlama hizmetinizle ilgileniyorum.",
        "Telefonum 0532 999 88 77",
        "İsmim Mehmet Yılmaz",
        "Yarın öğleden sonra saat 15:00 toplantı için uygun mu?"
    ],
    "senaryo_b_fikir_degistirme": [
        "Selamlar",
        "Web tasarım paketiniz ne kadar?",
        "Tamam onu almak istiyorum.",
        "Yok hayır vazgeçtim, web tasarım değil CRM hizmeti olsun.",
        "Bu arada pazar günleri açık mısınız?",
        "Anladım, o zaman Pazartesi sabah 10:00 olsun. İsmim Ayşe, tel: 0555 444 33 22"
    ],
    "senaryo_c_agresif_ve_sinir_zorlama": [
        "Hayırdır, siz de mi dolandırıcısınız her yer ajans doldu?",
        "Bana bedavaya logo yapar mısınız?",
        "Fiyatlarınız aşırı pahalı, kesin indirim yapmanız lazım.",
        "O zaman bana sadece diş beyazlatma yapın.",
        "Şaka şaka. Sosyal medya yönetimi için görüşelim. Adım Can, 0533 111 22 33, Cuma 14:00"
    ]
}

print("=== KAPSAMLI CANLI ÜRETİM TESTİ BAŞLIYOR ===\n")

for scenario_name, messages in SCENARIOS.items():
    sender_id = f"test_ux_{uuid.uuid4().hex[:6]}"
    print(f"\n--- {scenario_name.upper()} (User: {sender_id}) ---")
    
    for msg in messages:
        print(f"[USER] {msg}")
        
        payload = {
            "sender_id": sender_id, 
            "message_text": msg
        }
        
        try:
            resp = requests.post(f"{BASE_URL}{ENDPOINT}", json=payload, verify=False, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                reply = data.get("reply_text", "--- empty reply ---")
                print(f"[BOT] {reply}\n")
            else:
                print(f"[BOT/ERROR] HTTP {resp.status_code} - {resp.text}\n")
        except Exception as e:
            print(f"[BOT/ERROR] {str(e)}\n")
            
        time.sleep(15)

print("=== TESTLER TAMAMLANDI ===")
