import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Instead of relying on build_ai_first_decision escaping through overrides,
# we explicitly inject price check BEFORE is_service_term_clarification to bypass "nedir" overlap.
price_handler = '''
    if is_price_question(message_text):
        lowered = message_text.lower()
        found_service = None
        from app.main import get_config
        for s in get_config().get("service_catalog", []):
            if any(kw.lower() in lowered for kw in s.get("keywords", [])):
                found_service = s
                break
        
        if found_service and "price" in found_service:
            dummy_conv = conversation.copy() if conversation else {}
            dummy_conv["service"] = found_service["display"]
            decision["reply_text"] = build_contextual_price_reply(dummy_conv)
        else:
            decision["reply_text"] = build_contextual_price_reply(conversation)
            
        decision["intent"] = "price_question"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_service_term_clarification(message_text):'''

text = text.replace(
    '    if is_service_term_clarification(message_text):',
    price_handler,
    1 # Replace only the first occurrence in apply_ai_first_quality_overrides
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Patch applied")
