import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# First revert the wrong injection
wrong_inj = '''
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

text = text.replace(wrong_inj, '    if is_service_term_clarification(message_text):')

# Now inject correctly inside apply_ai_first_quality_overrides.
# We look for the exact signature of it in the function.
target_block = '''
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
'''

replacement_block = '''
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

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

    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
'''

text = text.replace(target_block, replacement_block)

# Wait... what if we already broke something with "return False return decision..."? Let's fix that.
# Line 8806 had a weird "return False decision["intent"]..." let's look for it
text = re.sub(r'        return False\n        decision\["intent"\] = "service_term_clarification"', '        decision["intent"] = "service_term_clarification"', text)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Exact patch applied")
