import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Identity Question Logic (Remove DOEL specific words, use config)
# Find is_assistant_identity_question
text = re.sub(
    r'def is_assistant_identity_question\(message_text: str\) -> bool:.*?return any\(term in lowered for term in identity_terms\)',
    '''def is_assistant_identity_question(message_text: str) -> bool:
    from app.main import get_config
    lowered = sanitize_text(message_text).lower()
    contact = get_config().get("human_contact_name", "yetkili").lower()
    identity_terms = ["bot musun", "bot mu", "yazilim mi", "yazılım mı", "yapay zeka mi", "yapay zeka mı", "otomasyon mu", "gerçek biri", "gercek biri", "insan mısın", "insan misin", "kiminle görüşüyorum", "kiminle gorusuyorum", "yetkili", "müşteri temsilcisi", "musteri temsilcisi", contact]
    return any(term in lowered for term in identity_terms)''',
    text, flags=re.DOTALL
)

# 2. Identity Question Reply (Remove DOEL specific answers, use generic handoff)
text = re.sub(
    r'def reply_answers_assistant_identity\(message_text: str\) -> str:.*?return "Ben DOEL Digital\'in yapay zeka tabanlı dijital asistanıyım.*?iletebilirim\."',
    '''def reply_answers_assistant_identity(message_text: str) -> str:
    from app.main import get_config
    conf = get_config()
    human = conf.get("human_contact_name", "yetkili")
    b_mode = conf.get("booking_mode", "randevu")
    return f"Ben dijital asistanım, {human} değilim. İsterseniz mesajınızı ekibe iletebilirim veya {b_mode} oluşturabiliriz."''',
    text, flags=re.DOTALL
)

# Also fix the duplicate at line 9083 if it's there (it's inside another function, maybe apply_ai_first_quality_overrides)
text = re.sub(
    r'return "Sorunuzu doğrudan cevaplayayım; bildiğim kısmı net aktarırım, emin olmadığım yerde de uydurmadan belirtirim\."',
    '''from app.main import get_config
        conf = get_config()
        return f"Ben dijital asistanım, {conf.get('human_contact_name', 'yetkili')} değilim. İsterseniz mesajınızı ekibe iletebilirim veya {conf.get('booking_mode', 'randevu')} oluşturabiliriz."''',
    text
)

# 3. Dynamic Service List in Greetings / Overview
text = re.sub(
    r'Ana hizmetlerimiz web tasarım, sosyal medya yönetimi, performans reklamları ve otomasyon/CRM sistemleri.*?alt çözümler de var\.',
    '{get_config().get("business_name", "İşletme")} olarak ana hizmetlerimiz: {", ".join([s["display"] for s in get_config().get("service_catalog", [])])}',
    text
)

text = re.sub(
    r'Buradayım\. Web, otomasyon, reklam veya sosyal medya tarafında neyi merak ettiğinizi yazarsanız net şekilde cevaplayayım\.',
    'Buradayım. Hangi hizmetimizle ({" ,".join([s["display"] for s in get_config().get("service_catalog", [])])}) ilgilendiğinizi yazarsanız net şekilde yardımcı olayım.',
    text
)

# 4. Contextual Price Reply logic enhancement: 
# If conversation.get("service") is empty, check text!
price_logic_replacement = '''def extract_service_from_text(text: str):
    from app.main import get_config
    lowered = text.lower()
    for s in get_config().get("service_catalog", []):
        if any(kw.lower() in lowered for kw in s.get("keywords", [])):
            return s
    return None

def build_contextual_price_reply(conversation: dict) -> str:
    from app.main import get_config
    
    # Check explicitly from text if conversation state didn't catch it
    # We pass message_text to pricing dynamically by extracting via history if needed, but since we modify apply_ai_first, let's fix it inside it.
'''
# Actually wait, build_contextual_price_reply is only called with `conversation` object, we don't have `message_text`!
# Let's fix apply_ai_first_quality_overrides where it calls build_contextual_price_reply.
text = re.sub(
    r'elif is_payment_question\(message_text\):\n\s*return build_contextual_price_reply\(conversation\)',
    '''elif is_payment_question(message_text):
        from app.main import get_config
        lowered = message_text.lower()
        found_service = None
        for s in get_config().get("service_catalog", []):
            if any(kw.lower() in lowered for kw in s.get("keywords", [])):
                found_service = s
                break
        if found_service and "price" in found_service:
            # Reconstruct dummy conversation with the detected service to force the price!
            dummy_conv = conversation.copy() if conversation else {}
            dummy_conv["service"] = found_service["display"]
            return build_contextual_price_reply(dummy_conv)
        return build_contextual_price_reply(conversation)''',
    text
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Main patched.")
