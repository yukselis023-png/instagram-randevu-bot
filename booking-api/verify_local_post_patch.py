from app import main
import json

print("\n--- 1. DOEL PRICING ---")
with open("app/config/doel.json", "r", encoding="utf-8") as f: doel_conf = json.load(f)
main.get_config = lambda: doel_conf
print(main.build_ai_first_decision("Web tasarım fiyatları nedir?", {}, [], {})['reply_text'])

print("\n--- 2. BEAUTY BOOKING START & ISOLATION ---")
with open("app/config/beauty.json", "r", encoding="utf-8") as f: beauty_conf = json.load(f)
main.get_config = lambda: beauty_conf
r = main.build_ai_first_decision("Randevu almak istiyorum", {}, [], {})['reply_text']
print(r)
assert "DOEL" not in r
assert "otomasyon" not in r

print("\n--- 3. BEAUTY SERVICES LIST ---")
r2 = main.build_ai_first_decision("Hizmetleriniz neler?", {}, [], {})['reply_text']
print(r2)
assert "otomasyon" not in r2

print("\n--- 4. DENTAL HANDOFF ---")
with open("app/config/dental.json", "r", encoding="utf-8") as f: dental_conf = json.load(f)
main.get_config = lambda: dental_conf
r3 = main.build_ai_first_decision("Doktorla konuşabilir miyim?", {}, [], {})['reply_text']
print(r3)
assert "doktorunuz değilim" in r3 or "doktorumuz değilim" in r3
