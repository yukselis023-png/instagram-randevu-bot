import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Clean up "Size nasıl yardımcı olabiliriz?"
code = code.replace(
    'return "İyidir, teşekkür ederim. Size nasıl yardımcı olabilirim?"',
    'return "İyidir, teşekkür ederim. İşletmeniz için hangi konuda destek aramıştınız?"'
)
code = code.replace(
    'return "İyiyim, teşekkür ederim. Size nasıl yardımcı olabiliriz?"',
    'return "İyiyim, teşekkür ederim. İşletmenizle ilgili bir projeniz mi var, yoksa genel bilgi mi almak istiyordunuz?"'
)
code = code.replace(
    'return "Size nasıl yardımcı olabilirim?"',
    'return "Hangi konuda detaylı bilgi almak istersiniz?"'
)
code = code.replace(
    'decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Size nasıl yardımcı olabilirim?"',
    'decision["reply_text"] = "Aleyküm selam, hoş geldiniz. İşletmeniz için web, reklam veya sosyal medya tarafında mı bilgi almak istemiştiniz?"'
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

# 2. Add Detailed Service & Ambiguous Appointment functions
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
    # If they clearly mention taking an appointment with us (gorusme, alalim) -> NOT ambiguous
    if any(w in lowered for w in ["almak", "olustur", "gorusme", "ayarla", "sizinle"]):
        return False
    # If they clearly mention the service (sistemi, otomasyon, entegrasyon) -> NOT ambiguous
    if any(w in lowered for w in ["sistemi", "otomasyon", "entegrasyon", "kurulum"]):
        return False
    return True

def build_ambiguous_appointment_reply() -> str:
    return "Randevu tarafında iki şekilde yardımcı olabiliriz: bizimle ön görüşme planlayabiliriz ya da işletmeniz için randevu/müşteri takip sistemi kurabiliriz. Hangisini merak ediyorsunuz?"

# Find where to inject
"""

# Inject before `def is_service_overview_question`
target_injection = "def is_service_overview_question("
if target_injection in code:
    code = code.replace(target_injection, detailed_svc_logic + "\n" + target_injection)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Basic generic replacements and intent injections done.")
