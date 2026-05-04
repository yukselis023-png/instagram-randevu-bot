import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# Fix correction checking
code = code.replace("if match_correction_message(message_text):", "if is_user_correction_message(message_text):")
code = code.replace("decision[\"reply_text\"] = build_correction_reply()", "decision[\"reply_text\"] = \"Anladım, düzelttiğiniz için teşekkürler. Nasıl yardımcı olabilirim?\"")

# Fix missing ACTIVE_BOOKING_STATES import/variable if needed?
# Wait, ACTIVE_BOOKING_STATES should be in the file
if "ACTIVE_BOOKING_STATES =" not in code:
    print("WARNING: ACTIVE_BOOKING_STATES not found")

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Vars fixed.")
