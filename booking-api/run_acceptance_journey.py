import requests
import uuid
import time
import urllib3
import json
urllib3.disable_warnings()

BASE_URL = "https://instagram-randevu-bot.onrender.com"
ENDPOINT = "/api/process-instagram-message"

MESSAGES = [
    "Merhaba",
    "Dövmeciyim, sitenizi gördüm merak edip yazdım",
    "Çok DM geliyor, hangi hizmet işime yarar?",
    "Otomasyon işime yarar mı?",
    "Ne kadar?",
    "Olur görüşelim",
    "Berkay Elbir",
    "05539088638",
    "Yarın 13:00",
    "Ödeme nasıl yapılıyor?",
    "Tamam teşekkürler"
]

sender_id = f"doel_acc_{uuid.uuid4().hex[:6]}"
print("=== LIVE PRODUCTION ACCEPTANCE TEST ===")
print(f"SENDER_ID: {sender_id}\n")

try:
    version_resp = requests.get(f"{BASE_URL}/version", verify=False)
    print(f"[VERSION] {version_resp.text}\n")
except:
    pass

for msg in MESSAGES:
    print(f"[USER] {msg}")
    payload = {"sender_id": sender_id, "message_text": msg}
    try:
        resp = requests.post(f"{BASE_URL}{ENDPOINT}", json=payload, verify=False, timeout=30)
        if resp.status_code == 200:
            print(f"[BOT] {resp.json().get('reply_text', '')}\n")
        else:
            print(f"[BOT/ERROR] HTTP {resp.status_code} - {resp.text}\n")
    except Exception as e:
        print(f"[BOT/ERROR] {str(e)}\n")

    time.sleep(15)
print("=== TEST TAMAMLANDI ===")
