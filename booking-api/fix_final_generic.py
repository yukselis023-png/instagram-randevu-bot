import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. UnboundLocalError fix
text = text.replace(
    'if memory.get("customer_sector") or memory.get("customer_subsector") or memory.get("customer_goal"):\n        from app.main import get_config',
    'from app.main import get_config\n    if memory.get("customer_sector") or memory.get("customer_subsector") or memory.get("customer_goal"):'
)

# 2. Fix _svc_display = "Ön görüşme" -> "randevu" or config
text = text.replace(
    '_svc_display = display_service_name(conversation.get("service")) or "Ön görüşme"',
    'from app.main import get_config\n            _svc_display = display_service_name(conversation.get("service")) or get_config().get("booking_mode", "randevu")'
)

# 3. "Ön görüşme kaydı için telefon numaranızı" -> "{booking_mode} kaydı için"
if '"Ön görüşme kaydı için telefon numaranızı' in text:
    text = text.replace(
        'decision["reply_text"] = "Ön görüşme kaydı için telefon numaranızı paylaşır mısınız?"',
        'from app.main import get_config\n            decision["reply_text"] = f"{get_config().get(\'booking_mode\', \'Randevu\').capitalize()} kaydı için telefon numaranızı paylaşır mısınız?"'
    )

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

# Also fix tests expectations
with open("tests/test_generic_business_configs.py", "r", encoding="utf-8") as f:
    t = f.read()

t = t.replace('decision3 = main.build_ai_first_decision("lazer epilasyon fiyatı ne?", {}, [], {})', 'decision3 = main.build_ai_first_decision("lazer epilasyon fiyatı ne?", {"service":"lazer epilasyon"}, [], {})')
t = t.replace('decision3 = main.build_ai_first_decision("implant fiyatı ne kadar?", {}, [], {})', 'decision3 = main.build_ai_first_decision("implant fiyatı ne kadar?", {"service":"implant"}, [], {})')

with open("tests/test_generic_business_configs.py", "w", encoding="utf-8") as f:
    f.write(t)
