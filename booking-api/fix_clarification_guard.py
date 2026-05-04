import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

target = '''def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()'''

replacement = '''def is_service_term_clarification(text: str) -> bool:
    try:
        if is_price_question(text):
            return False
            
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()'''

text = text.replace(target, replacement)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

