import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

target = """    if is_assistant_identity_question(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)"""

replacement = """    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_assistant_identity_question(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)"""

if target in code:
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(code.replace(target, replacement))
    print("M5 patch applied successfully.")
else:
    print("Target block not found.")
