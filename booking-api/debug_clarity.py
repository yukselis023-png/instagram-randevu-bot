from app import main
import json

with open("app/config/beauty.json", "r", encoding="utf-8") as f: BEAUTY_CONF = json.load(f)

# Hard override get_config locally
main.get_config = lambda: BEAUTY_CONF

msg = "hydrafacial nedir?"
print("CLARITY:", main.is_service_term_clarification(msg))
print("REPLY:", main.build_service_term_clarification_reply(msg))

decision = main.build_ai_first_decision(msg, {}, [], {})
print("DECISION INTENT:", decision.get("intent"))
print("DECISION REPLY:", decision.get("reply_text"))
