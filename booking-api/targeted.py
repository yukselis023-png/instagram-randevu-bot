import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Clean up "Size nasıl yardımcı olabiliriz?" (Without breaking length)
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
    'decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Nasıl yardımcı olabiliriz?"'
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

# 2. Ambiguity & Detailed Services Logic properly added
detailed_svc_logic = """
def is_detailed_service_question(text: str, history: list) -> bool:
    lowered = sanitize_text(text).lower()
    is_expanding = any(w in lowered for w in ["bu kadar mi", "bu kadar mı", "baska", "başka", "daha detayli", "daha detaylı", "alt hizmet"])
    if not is_expanding:
        return False
    recent_outbound = get_last_outbound_text(history).lower()
    has_recent_overview = any(w in recent_outbound for w in ["web tasarim", "reklam", "otomasyon", "sosyal medya"])
    return has_recent_overview or "hizmet" in lowered

def build_detailed_service_reply() -> str:
    return "Ana hizmetlerimiz web tasarım, sosyal medya yönetimi, performans reklamları ve otomasyon/CRM sistemleri. Bunların altında Instagram yönetimi, reklam kurulumu, müşteri takip sistemi, randevu akışı, landing page ve web sitesi gibi alt çözümler de var."

def is_ambiguous_appointment_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if "randevu" not in lowered:
        return False
    if any(w in lowered for w in ["almak", "olustur", "gorusme", "ayarla", "sizinle"]):
        return False
    if any(w in lowered for w in ["sistemi", "otomasyon", "entegrasyon", "kurulum"]):
        return False
    return True

def build_ambiguous_appointment_reply() -> str:
    return "Randevu tarafında iki şekilde yardımcı olabiliriz: bizimle ön görüşme planlayabiliriz ya da işletmeniz için randevu/müşteri takip sistemi kurabiliriz. Hangisini merak ediyorsunuz?"

def is_service_overview_question("""

target_injection = "def is_service_overview_question("
if target_injection in code:
    code = code.replace(target_injection, detailed_svc_logic)


# 3. Add to overrides but preserve dictionary formats completely
overrides_target = """    if is_simple_greeting(message_text):
        decision["reply_text"] = build_natural_greeting_reply()"""

overrides_new = """    if is_ambiguous_appointment_question(message_text):
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

    if is_simple_greeting(message_text):
        decision["reply_text"] = build_natural_greeting_reply()"""

if overrides_target in code:
    code = code.replace(overrides_target, overrides_new)

# 4. Fix Guard properly (test-friendly)
# We want Guard to override AI if failed. So if fail_reasons has items, replace `reply` with safe response!
guard_target = """    if fail_reasons:
        return {"ok": False, "reasons": fail_reasons, "repaired": False}"""

guard_replacement = """    if fail_reasons:
        # DO NOT RETURN AI RESPONSE!
        safe = "Hangi konuyla doğrudan ilgilendiğinizi iletebilir misiniz?"
        if "too_long" in fail_reasons: safe = "Detayları ön görüşmemizde birlikte değerlendirmek daha sağlıklı olacaktır. Ne zaman planlayalım?"
        if "repeated_time_block" in fail_reasons: safe = "Seçtiğiniz saat doluydu. Lütfen farklı saat önerebilir misiniz?"
        return {"ok": False, "reasons": fail_reasons, "repaired": True, "reply_text": safe}"""

if guard_target in code:
    code = code.replace(guard_target, guard_replacement)

# And below it is `return {"ok": True, "reasons": [], "repaired": True}` when repaired via regex
# We must ensure test expects it right. The safest approach is just changing the text inside the function: `_reply = safe_reply`

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Targeted patch executed safely.")
