import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Ambiguity & Details Helpers
helpers = """def is_detailed_service_question(text: str, history: list) -> bool:
    try:
        lowered = sanitize_text(text).lower()
        is_expanding = any(w in lowered for w in ["bu kadar mi", "bu kadar mı", "baska", "başka", "daha detayli", "daha detaylı", "alt hizmet"])
        if not is_expanding:
            return False
        recent_outbound = get_last_outbound_text(history).lower() if history else ""
        has_recent_overview = any(w in recent_outbound for w in ["web tasarim", "reklam", "otomasyon", "sosyal medya"])
        return has_recent_overview or "hizmet" in lowered
    except: return False

def build_detailed_service_reply() -> str:
    return "Ana hizmetlerimiz web tasarım, sosyal medya yönetimi, performans reklamları ve otomasyon/CRM sistemleri. Bunların altında Instagram yönetimi, reklam kurulumu, müşteri takip sistemi, randevu akışı, landing page ve web sitesi gibi alt çözümler de var."

def is_ambiguous_appointment_question(text: str) -> bool:
    try:
        lowered = sanitize_text(text).lower()
        if "randevu" not in lowered: return False
        if any(w in lowered for w in ["almak", "olustur", "gorusme", "ayarla", "sizinle"]): return False
        if any(w in lowered for w in ["sistemi", "otomasyon", "entegrasyon", "kurulum"]): return False
        return True
    except: return False

def build_ambiguous_appointment_reply() -> str:
    return "Randevu tarafında iki şekilde yardımcı olabiliriz: bizimle ön görüşme planlayabiliriz ya da işletmeniz için randevu/müşteri takip sistemi kurabiliriz. Hangisini merak ediyorsunuz?"

def is_service_overview_question("""
code = code.replace("def is_service_overview_question(", helpers)

# 2. Block Recommendation
rec_target = """def should_use_customer_recommendation_override(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> bool:"""
rec_replacement = rec_target + """
    if is_ambiguous_appointment_question(message_text):
        return False
"""
code = code.replace(rec_target, rec_replacement)

# 3. Add to overrides top
ov_target = "def apply_ai_first_quality_overrides(\n    message_text: str,\n    decision: dict[str, Any],\n    conversation: dict[str, Any],\n    history: list[dict[str, Any]] | None = None,\n) -> dict[str, Any]:"
ov_replacement = ov_target + """
    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
"""
# Some IDEs use different indentation, lets use a simple replace
if "def apply_ai_first_quality_overrides" in code:
    ov_target_2 = "def apply_ai_first_quality_overrides(message_text: str, decision: dict[str, Any], conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:"
    if ov_target_2 in code:
        code = code.replace(ov_target_2, ov_replacement.replace("def apply_ai_first_quality_overrides(\n    message_text: str,\n    decision: dict[str, Any],\n    conversation: dict[str, Any],\n    history: list[dict[str, Any]] | None = None,\n) -> dict[str, Any]:", ov_target_2))
    else:
        # Just insert it randomly inside the function body at beginning
        inj_target = """    _active_state = str(conversation.get("state") or "")"""
        inj_replacement = inj_target + """
    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision"""
        code = code.replace(inj_target, inj_replacement)

# 4. Remove generic 'Size nasil yardimci olabiliriz'
code = code.replace(
    'return "İyidir, teşekkür ederim. Size nasıl yardımcı olabilirim?"',
    'return "İyidir, teşekkür ederim. İşletmeniz için hangi alanda dijital destek aramıştınız?"'
)
code = code.replace(
    'return "İyiyim, teşekkür ederim. Size nasıl yardımcı olabiliriz?"',
    'return "İyiyim, teşekkür ederim. İşletmenizle ilgili bir projeniz mi var, yoksa genel bilgi mi istiyorsunuz?"'
)
code = code.replace(
    'return "Size nasıl yardımcı olabilirim?"',
    'return "Hangi konuyla doğrudan ilgilenmektesiniz?"'
)
code = code.replace(
    'decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Size nasıl yardımcı olabilirim?"',
    'decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Hangi alanda işletmenizi geliştirmek istersiniz?"'
)
code = code.replace(
    'decision["reply_text"] = "Merhaba, teşekkür ederiz. Size nasıl yardımcı olabiliriz?"',
    'decision["reply_text"] = "Merhaba, teşekkür ederiz. Hangi hizmetimizle ilgili detay istersiniz?"'
)
code = code.replace(
    'return "Merhaba, buradayım. Size nasıl yardımcı olabilirim?"',
    'return "Merhaba, buradayım. Konuyu iletebilirsiniz."'
)
code = code.replace(
    'return "Evet, buradayım. Nasıl yardımcı olabilirim?"',
    'return "Evet, buradayım. Konuyu iletebilirsiniz."'
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Safely patched.")
