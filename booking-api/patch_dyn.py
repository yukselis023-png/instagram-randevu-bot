import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update is_service_term_clarification
p_clarify = r"def is_service_term_clarification\(text: str\) -> bool:.*?return False"
new_clarify = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey", "aynı şey", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir", "ne işe yarar", "ne ise yarar"]
        if not any(t in lowered for t in triggers):
            return False
            
        catalog = get_config().get('service_catalog', [])
        for svc in catalog:
            for kw in svc.get('keywords', []):
                if kw in lowered:
                    return True
        return False
    except:
        return False"""
text = re.sub(p_clarify, new_clarify, text, flags=re.DOTALL)

# 2. Update build_service_term_clarification_reply
p_repl_clarify = r"def build_service_term_clarification_reply\(text: str\) -> str:.*?return \"[^\"]*ön görüşmemizde[^\"]*\""
new_repl_clarify = """def build_service_term_clarification_reply(text: str) -> str:
    from app.main import sanitize_text, get_config
    lowered = sanitize_text(text).lower()
    catalog = get_config().get('service_catalog', [])
    for svc in catalog:
        for kw in svc.get('keywords', []):
            if kw in lowered:
                if 'clarification' in svc:
                    return svc['clarification']
    return f"Bahsettiğiniz konu {get_config().get('business_name', '')} hizmetleri kapsamındadır. Detaylı teknik bilgiyi uzman ekibimizle görüşerek alabilirsiniz." """
text = re.sub(p_repl_clarify, new_repl_clarify, text, flags=re.DOTALL)
# It might leave the except block from old code. Let's just fix it by matching the whole function body
p_repl_clarify2 = r"def build_service_term_clarification_reply\(text: str\) -> str:(.*?)\n(?:def |\Z)"
def replacer2(m):
    return new_repl_clarify + "\n\n"
text = re.sub(p_repl_clarify2, replacer2, text, flags=re.DOTALL)

# 3. Update build_business_fit_reply
p_fit = r"def build_business_fit_reply\((.*?)\) -> str:(.*?)\n(?:def |\Z)"
new_fit = """def build_business_fit_reply(
    conversation: dict,
    message_text: str | None = None,
    history: list | None = None,
) -> str:
    from app.main import sanitize_text, get_config
    catalog = get_config().get('service_catalog', [])
    if message_text:
        lowered = sanitize_text(message_text).lower()
        for svc in catalog:
            for kw in svc.get('keywords', []):
                if kw in lowered and 'fit_description' in svc:
                    return svc['fit_description']
    return "Bu hizmetimiz hedeflerinize uygun olabilir. Detayları kısa bir görüşmede netleştirebiliriz."
\n"""

text = re.sub(p_fit, new_fit, text, flags=re.DOTALL)

# 4. Also fix is_business_fit_question
p_is_fit = r"def is_business_fit_question\(text: str\) -> bool:(.*?)\n(?:def |\Z)"
new_is_fit = """def is_business_fit_question(text: str) -> bool:
    from app.main import sanitize_text, get_config
    lowered = sanitize_text(text).lower()
    triggers = ["uygun mu", "uyar mi", "uyar mı", "isime yarar", "işime yarar", "faydali olur", "faydalı olur", "mantikli mi", "mantıklı mı"]
    if not any(t in lowered for t in triggers):
        return False
    catalog = get_config().get('service_catalog', [])
    for svc in catalog:
        for kw in svc.get('keywords', []):
            if kw in lowered:
                return True
    return False
\n"""
text = re.sub(p_is_fit, new_is_fit, text, flags=re.DOTALL)


with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
