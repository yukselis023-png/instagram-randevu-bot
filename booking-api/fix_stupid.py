with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix confirmed_booking_should_take_over_message back to correct logic
def patch_confirmed():
    global text
    parts = text.split("def confirmed_booking_should_take_over_message")
    if len(parts) < 2: return
    head = parts[0]
    tail_raw = parts[1]
    
    idx = tail_raw.find("def is_post_confirmation_clarification")
    if idx == -1: idx = text.find("def wants_new_booking")
    
    body = tail_raw[:idx]
    tail2 = tail_raw[idx:]
    
    body = body.replace('if is_service_term_clarification(message_text):\n        return False', 'if is_service_term_clarification(message_text):\n        return False')
    # wait that one is correct!
    text = head + "def confirmed_booking_should_take_over_message" + body + tail2

patch_confirmed()

# Now fix the router back!
def patch_router():
    global text
    parts = text.split("def apply_ai_first_quality_overrides(")
    if len(parts) < 2: return
    head = parts[0]
    tail_raw = parts[1]
    
    idx = tail_raw.find("def build_ai_first_emergency_reply")
    body = tail_raw[:idx]
    tail2 = tail_raw[idx:]
    
    # restore router return
    body = body.replace(
        'if is_service_term_clarification(message_text):\n        return False',
        'if is_service_term_clarification(message_text):\n        decision["reply_text"] = build_service_term_clarification_reply(message_text)\n        decision["intent"] = "service_term_clarification"\n        decision["booking_intent"] = False\n        decision["missing_fields"] = []\n        decision["should_reply"] = True\n        return decision'
    )
    
    text = head + "def apply_ai_first_quality_overrides(" + body + tail2

patch_router()

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("done")
