import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Update build_vague_price_reply
old_vague = """def build_vague_price_reply(conversation: dict[str, Any] | None = None) -> str:
    return "Fiyat seçilecek hizmete göre değişir; web, reklam ve otomasyon ayrı kapsamlarla hazırlanıyor. İhtiyacınıza uygun başlangıcı netleştirirsek doğru fiyatı çıkarabiliriz." """
new_vague = """def build_vague_price_reply(conversation: dict[str, Any] | None = None) -> str:
    from app.main import get_config
    return f"Fiyat seçilecek hizmete göre değişiyor. Detayları kısa bir {get_config().get('booking_mode', 'görüşme')}de netleştirebiliriz." """
if old_vague in text:
    text = text.replace(old_vague, new_vague)
else:
    # use regex
    text = re.sub(r"def build_vague_price_reply.*?return \"Fiyat.*?\"", new_vague, text, flags=re.DOTALL)

# 2. build_ambiguous_appointment_reply
old_amb = """def build_ambiguous_appointment_reply() -> str:
    return "Randevu tarafında iki şekilde yardımcı olabiliriz: bizimle ön görüşme planlayabiliriz ya da işletmeniz için randevu/müşteri takip sistemi kurabiliriz. Hangisini merak ediyorsunuz?"""
new_amb = """def build_ambiguous_appointment_reply() -> str:
    from app.main import get_config
    labels = get_config().get('appointment_service_labels', ['randevu'])
    return f"Tabii, yardımcı olabiliriz. {labels[0].capitalize()} planlamak için mi yazmıştınız?"""
if old_amb in text:
    text = text.replace(old_amb, new_amb)
else:
    text = re.sub(r"def build_ambiguous_appointment_reply.*?return \".*?\"", new_amb, text, flags=re.DOTALL)


# 3. Update all keywords iterators to sanitize keywords before matching.
# We will use regex to find the blocks:
# for kw in svc.get("keywords", []):
#    if kw in lowered:
def add_sanitize(match):
    return """for kw in svc.get("keywords", []):
                kw_clean = sanitize_text(kw).lower()
                if kw_clean in lowered:"""

text = re.sub(r"for kw in svc\.get\([\"\']keywords[\"\'], \[\]\):\s*if kw in lowered:", add_sanitize, text)

# For the fit_description one:
def add_sanitize_fit(match):
    return """for kw in svc.get("keywords", []):
                kw_clean = sanitize_text(kw).lower()
                if kw_clean in lowered and "fit_description" in svc:"""
text = re.sub(r"for kw in svc\.get\([\"\']keywords[\"\'], \[\]\):\s*if kw in lowered and [\"\']fit_description[\"\'] in svc:", add_sanitize_fit, text)


with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
