import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

target = """    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()"""

replacement = """    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()"""

if target in code:
    code = code.replace(target, replacement)
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(code)
    print("Overrides patched successfully!")
else:
    print("Target block not found.")
