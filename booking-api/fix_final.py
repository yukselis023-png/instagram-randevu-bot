import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. ADD NEW HELPER FUNCTIONS
helpers = """def is_detailed_service_question(text: str, history: list) -> bool:
    try:
        from app.main import sanitize_text, get_last_outbound_text
        lowered = sanitize_text(text).lower()
        if not any(w in lowered for w in ["bu kadar mi", "bu kadar mı", "baska", "başka", "daha detayli", "daha detaylı", "alt hizmet"]): return False
        r = get_last_outbound_text(history).lower() if history else ""
        return any(w in r for w in ["web tasarim", "reklam", "otomasyon", "sosyal medya"]) or "hizmet" in lowered
    except: return False

def build_detailed_service_reply() -> str:
    return "Ana hizmetlerimiz web tasarım, sosyal medya yönetimi, performans reklamları ve otomasyon/CRM sistemleri. Bunların altında Instagram yönetimi, reklam kurulumu, müşteri takip sistemi, randevu akışı, landing page ve web sitesi gibi alt çözümler de var."

def is_ambiguous_appointment_question(text: str) -> bool:
    try:
        from app.main import sanitize_text
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

# 2. Block Ambiguous Recommendations!
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

# 3. Add to overrides logic
ov_target = """def apply_ai_first_quality_overrides(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decision["reply_text"] = cleanup_ai_first_reply_text(decision.get("reply_text"))"""
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
code = code.replace(ov_target, ov_replacement)

# 4. Clean "Size nasil yardimci olabiliriz" loops natively (Replace mostly strings, no complex logic)
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

# 5. Fix final_answer_quality_guard
guard_target = """def final_answer_quality_guard(
    message_text: str,
    reply_text: str | None,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
    decision_label: str | None = None,
) -> dict[str, Any]:"""
guard_replacement = guard_target + """
    whitelist = [
        "correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation"
    ]
    if decision_label in whitelist:
        return {"passed": True, "reason": "whitelisted_intent"}
"""
code = code.replace(guard_target, guard_replacement)

# To actually override the AI output if final answer fails:
repair_target = """    repaired = build_safe_reply_builder(message_text, conversation, history, decision_label)
    second = final_answer_quality_guard(message_text, repaired, conversation, history, decision_label)"""
repair_replacement = """
    # IF NOT WHITELISTED AND IT FAILED -> WE DETACH AI HALLUCINATION AND USE DETERMINISTIC ONLY
    # Create safe contextual string instead of hallucination
    fail_rs = first["reason"]
    if fail_rs == "too_long": safe_rep = "Detayları ön görüşmemizde birlikte değerlendirmek daha sağlıklı olacaktır. Ne zaman planlayalım?"
    elif fail_rs == "repeated_time_block": safe_rep = "Seçtiğiniz saat doluydu. Lütfen farklı saat önerebilir misiniz?"
    elif fail_rs == "repeated_greeting": safe_rep = "İşletmeniz için hangi alanda destek arıyorsunuz?"
    else: safe_rep = "Bu konuyu detaylandırmak için iletişimi başlatabilir misiniz?"
    
    return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": fail_rs}
    
    repaired = build_safe_reply_builder(message_text, conversation, history, decision_label)
    second = final_answer_quality_guard(message_text, repaired, conversation, history, decision_label)"""
code = code.replace(repair_target, repair_replacement)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Finished!")
