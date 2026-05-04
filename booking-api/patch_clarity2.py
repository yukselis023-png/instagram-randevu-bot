import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

new_re = """def build_service_term_clarification_reply(text: str) -> str:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        catalog = get_config().get("service_catalog", [])
        for svc in catalog:
            for kw in svc.get("keywords", []):
                if kw in lowered:
                    if "clarification" in svc:
                        return svc["clarification"]
        return f"Bahsettiğiniz konu {get_config().get('business_name', '')} hizmetleri kapsamındadır. Detaylı bilgiyi değerlendirebiliriz."
    except:
        return "Detaylı bilgiyi ön görüşmemizde birlikte değerlendirebiliriz."
"""

# Replace specifically from "def build_service_term_clarification_reply" up to "def is_service_overview_question"
p = r"def build_service_term_clarification_reply\(text: str\) -> str:.*?(?=def is_service_overview_question)"
text = re.sub(p, new_re + "\n", text, flags=re.DOTALL)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
