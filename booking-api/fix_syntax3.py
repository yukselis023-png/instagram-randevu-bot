with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace empty if blocks if any
# The replace removed the print string but left the `if is_price_question(message_text):` empty line perhaps?
import re
text = re.sub(r'    if is_price_question\(message_text\):\n\s*if is_price_question\(message_text\):', '    if is_price_question(message_text):', text)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

