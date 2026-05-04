import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update build_services_overview_reply
# It spans from def build_services_overview_reply... to next function
text = re.sub(
    r'def build_services_overview_reply.*?(?=def |@)',
    '''def build_services_overview_reply(history: list[dict[str, Any]] | None = None) -> str:
    from app.main import get_config
    conf = get_config()
    bname = conf.get("business_name", "İşletmemiz")
    btype = conf.get("business_type", "hizmet platformu")
    katalog = conf.get("service_catalog", [])
    if not katalog:
        return f"{bname} olarak çeşitli hizmetlerimiz bulunmaktadır. Hangi alanda destek arıyorsunuz?"
    
    hizmet_isimleri = ", ".join([s.get("display", "") for s in katalog])
    hizmet_detaylari = " ".join([f"({s.get('display')}: {s.get('summary', '')})" for s in katalog])
    
    # Kaskad/aşırı uzatmamak için kısa
    return f"{bname} olarak ana hizmetlerimiz: {hizmet_isimleri}. Eğer bunlardan biri ilginizi çekiyorsa detaylarını aktarabilirim."

''', text, flags=re.DOTALL
)

# Wait! There's also `is_detailed_service_question` maybe?
# Let's fix apply_ai_first_quality_overrides precedence for Price!
# Put `is_payment_question` BEFORE `is_service_term_clarification`
text = re.sub(
    r'(elif is_service_term_clarification\(message_text\):.*?return build_term_clarification_reply\(message_text\)\n)',
    r'',
    text, flags=re.DOTALL
)
# Re-insert clarification after payment
text = re.sub(
    r'(elif is_payment_question\(message_text\):.*?return build_contextual_price_reply\(conversation\)\n)',
    r'\1\n    elif is_service_term_clarification(message_text):\n        return build_term_clarification_reply(message_text)\n',
    text, flags=re.DOTALL
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
