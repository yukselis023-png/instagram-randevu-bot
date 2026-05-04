import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    'return "Merhaba, yardımcı olayım. {get_config().get("booking_mode", "randevu")} işleminiz için hangi hizmetle ilgilendiğinizi belirtebilir misiniz?"',
    'return f"Merhaba, yardımcı olayım. {get_config().get(\'booking_mode\', \'randevu\')} işleminiz için hangi hizmetle ilgilendiğinizi belirtebilir misiniz?"'
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
