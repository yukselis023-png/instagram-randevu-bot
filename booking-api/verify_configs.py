import urllib.request, json
from app import main

print("=== LIVE PROD (DOEL) ===")
questions_doel = [
    "CRM işime yarar mı?",
    "Web tasarım fiyatları nedir?",
    "Web tasarımla web sitesi aynı şey değil mi?",
    "Otomasyon ne demek?"
]

url = 'https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
for q in questions_doel:
    payload = {'sender_id': 'test_verify_user1', 'message_text': q, 'channel': 'instagram'}
    req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers={'Content-Type': 'application/json'})
    try:
        resp = urllib.request.urlopen(req)
        print(f"Q: {q}\nA: {json.loads(resp.read().decode('utf-8')).get('reply_text')}\n")
    except Exception as e:
        print(f"Q: {q}\nA: ERROR {e}\n")

print("=== LOCAL STAGING (BEAUTY SALON) ===")
with open("app/config/beauty.json", "r", encoding="utf-8") as f: beauty = json.load(f)
def ask_local(msg, cfg):
    main.get_config = lambda: cfg
    main.DOEL_SERVICE_CATALOG = cfg.get("service_catalog", [])
    conv = {"state": "new", "memory_state": {}}
    res = main.build_ai_first_decision(msg, conv, [], {})
    return res["reply_text"]

questions_beauty = ["Hydrafacial ne demek?", "Lazer epilasyon fiyatı ne?", "Randevu almak istiyorum"]
for q in questions_beauty:
    print(f"Q: {q}\nA: {ask_local(q, beauty)}\n")

print("=== LOCAL STAGING (DENTAL CLINIC) ===")
with open("app/config/dental.json", "r", encoding="utf-8") as f: dental = json.load(f)
questions_dental = ["İmplant ne demek?", "Muayene ücreti ne kadar?", "Doktorla konuşabilir miyim?"]
for q in questions_dental:
    print(f"Q: {q}\nA: {ask_local(q, dental)}\n")
