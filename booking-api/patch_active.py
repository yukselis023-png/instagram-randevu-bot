import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# I will find the 'if _active_state in ACTIVE_BOOKING_STATES and not is_general_information_request' line and add 'and not decision.get("intent", "").startswith("booking_collect")'
old = 'and not is_invalid_name_attempt(message_text, _active_state):'
new = 'and not is_invalid_name_attempt(message_text, _active_state) and not str(decision.get("intent") or "").startswith("booking_collect") and not str(decision.get("intent") or "") == "booking_confirmed":'

if old in text:
    text = text.replace(old, new)
else:
    print("Could not find line")

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
