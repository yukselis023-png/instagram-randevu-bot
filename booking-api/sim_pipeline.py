import json
from app import main

# Mock memory
class MockDB:
    def __init__(self):
        self.conv = {"state": "collect_service", "service": None, "customer_sector": None, "history": []}
    def get_user_conversation(self, uid):
        return self.conv
    def save_user_conversation(self, uid, data):
        self.conv = data

main.db = MockDB()

def sim(msg, conf_file):
    with open(f"app/config/{conf_file}.json", "r", encoding="utf-8") as f:
        conf = json.load(f)
    main.get_config = lambda: conf
    main.DOEL_SERVICE_CATALOG = conf.get("service_catalog", [])
    
    reply = main.generate_reply(msg, "mock_user", [], {})
    print(reply)

print("--- DOEL: Web tasarım fiyatları nedir? ---")
sim("Web tasarım fiyatları nedir?", "doel")

print("--- BEAUTY: Randevu almak istiyorum ---")
sim("Randevu almak istiyorum", "beauty")

print("--- BEAUTY: Hizmetleriniz neler? ---")
sim("Hizmetleriniz neler?", "beauty")

print("--- DENTAL: Doktorla konuşabilir miyim? ---")
sim("Doktorla konuşabilir miyim?", "dental")
