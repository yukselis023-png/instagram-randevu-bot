from app import main
import json
with open("app/config/dental.json", "r", encoding="utf-8") as f: DENTAL_CONF = json.load(f)
main.get_config = lambda: DENTAL_CONF

conv = {"state": "collect_name", "memory_state": {}, "missing_fields": [], "service": "İmplant Tedavisi"}
decision = main.build_ai_first_decision("Yüksel Yiğit", conv, [], {})
print("INTENT:", decision.get("intent"))
print("REPLY:", decision.get("reply_text"))
