import urllib.request, json
import time

def call_live(sender, text):
    url = 'https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
    payload = {'sender_id': sender, 'message_text': text, 'channel': 'instagram'}
    req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
    try:
        resp = urllib.request.urlopen(req)
        print(f"User: {text}\nBot:  {json.loads(resp.read().decode('utf-8'))['reply_text']}\n")
    except Exception as e:
        print(f"API Error for '{text}': {e}\n")

print("\n--- 3. LIVE DOEL TESTS ---")
call_live("doel_test_user_1", "CRM işime yarar mı?")
call_live("doel_test_user_1", "Web tasarım fiyatları nedir?")
call_live("doel_test_user_1", "Web tasarımla web sitesi aynı şey değil mi?")
call_live("doel_test_user_1", "Otomasyon ne demek?")

print("--- 4. LOCAL BEAUTY SALON TESTS ---")
from app import main

with open("app/config/beauty.json", "r", encoding="utf-8") as f: beauty_conf = json.load(f)
main.get_config = lambda: beauty_conf
main.DOEL_SERVICE_CATALOG = beauty_conf.get("service_catalog", [])
print(f"User: Hydrafacial ne demek?\nBot:  {main.build_ai_first_decision('Hydrafacial ne demek?', {}, [], {})['reply_text']}\n")
print(f"User: Lazer epilasyon fiyatı ne?\nBot:  {main.build_ai_first_decision('Lazer epilasyon fiyatı ne?', {'service': 'lazer epilasyon'}, [], {})['reply_text']}\n")
print(f"User: Randevu almak istiyorum\nBot:  {main.build_ai_first_decision('Randevu almak istiyorum', {}, [], {})['reply_text']}\n")

print("--- 5. LOCAL DENTAL CLINIC TESTS ---")
with open("app/config/dental.json", "r", encoding="utf-8") as f: dental_conf = json.load(f)
main.get_config = lambda: dental_conf
main.DOEL_SERVICE_CATALOG = dental_conf.get("service_catalog", [])
print(f"User: İmplant ne demek?\nBot:  {main.build_ai_first_decision('İmplant ne demek?', {}, [], {})['reply_text']}\n")
print(f"User: Muayene ücreti ne kadar?\nBot:  {main.build_ai_first_decision('Muayene ücreti ne kadar?', {'service': None}, [], {})['reply_text']}\n")
print(f"User: Doktorla konuşabilir miyim?\nBot:  {main.build_ai_first_decision('Doktorla konuşabilir miyim?', {}, [], {})['reply_text']}\n")
