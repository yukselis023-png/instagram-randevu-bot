import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Inject Config Import at the top
if "from app.config.settings import get_config" not in code:
    code = code.replace("from typing import Any", "from typing import Any\nfrom app.config.settings import get_config")

# 2. Swap DOEL_SERVICE_CATALOG with get_config().get("service_catalog")
if "DOEL_SERVICE_CATALOG =" in code:
    # Disable the hardcoded array
    code = re.sub(r"DOEL_SERVICE_CATALOG = \[\s*\{.*?\}\s*\]", "DOEL_SERVICE_CATALOG = get_config().get('service_catalog', [])", code, flags=re.DOTALL)
    # If the sub didn't find the whole array, let's just do a simpler replace. Actually, I can just leave DOEL_SERVICE_CATALOG pointing to the config
    
# 3. Fix clarification question logic to be fully generic
old_clarification = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey", "aynı şey", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing", "web", "sosyal medya", "reklam", "performans"]
        return any(term in lowered for term in terms)
    except:
        return False"""

new_clarification = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey", "aynı şey", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
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
        
code = code.replace(old_clarification, new_clarification)

# 4. Fix clarification reply to be generic
old_cl_reply = """def build_service_term_clarification_reply(text: str) -> str:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        
        if "otomasyon" in lowered or "bot" in lowered:
            return "Otomasyon, gelen mesajları anında yanıtlayan, tekrar eden işleri insansız çözen ve müşterileri kaydeden yazılım altyapısıdır."
        elif "crm" in lowered:
            return "CRM (Müşteri İlişkileri Yönetimi), müşterilerinizin verilerini, randevularını ve görüşme geçmişlerini tek bir yerden yönetmenizi sağlayan sistemdir."
        elif "landing" in lowered:
            return "Landing page, tek bir ürüne veya hizmete odaklanan, ziyaretçiyi doğrudan satın almaya veya form doldurmaya yönlendiren özel tasarlanmış açılış sayfasıdır."
        elif "web tasarim" in lowered or "web sitesi" in lowered or "website" in lowered or "web" in lowered:
            return "Evet, çoğu zaman aynı anlamda kullanılır. Web tasarım, web sitesinin görünüm, yapı ve kullanıcı deneyimi tarafını ifade eder."
        elif "sosyal medya" in lowered:
            return "Sosyal medya yönetimi, hesabınızın düzenli içeriklerle profesyonel görünmesi, takipçilerle etkileşim ve güven oluşturması sürecidir."
        elif "reklam" in lowered or "performans" in lowered:
            return "Performans pazarlama, doğrudan satış veya randevu getirmeye odaklı, ölçülebilir dijital reklam kampanyaları yönetimidir."
            
        return "Bahsettiğiniz terim dijital süreçlerin bir parçasıdır. İşletmeniz için en uygun stratejiyi ön görüşmemizde birlikte belirleyebiliriz."
    except:
        return "Bu teknik bir süreçtir, dilerseniz ön görüşmemizde detaylıca açıklayabiliriz."
"""

new_cl_reply = """def build_service_term_clarification_reply(text: str) -> str:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        catalog = get_config().get('service_catalog', [])
        
        # Try to find specific clarification from config
        for svc in catalog:
            for kw in svc.get('keywords', []):
                if kw in lowered:
                    if 'clarification' in svc:
                        return svc['clarification']
                        
        # Fallback clarification
        return f"Bahsettiğiniz konu {get_config().get('business_name', '')} hizmetleri kapsamındadır. Detaylı teknik bilgiyi uzman ekibimizle görüşerek alabilirsiniz."
    except:
        return "Detaylı bilgiyi ön görüşmemizde/randevunuzda birlikte değerlendirebiliriz."
"""
code = code.replace(old_cl_reply, new_cl_reply)

# 5. Fix Business Fit Logic (DOEL hardcodes)
old_fit_logic = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
) -> str:
    from app.main import sanitize_text
    
    # Generic service intent extraction
    if message_text:
        lowered = sanitize_text(message_text).lower()
        if "crm" in lowered:
            return "CRM, gelen müşteri taleplerini, randevuları ve takip süreçlerini düzenlemek istiyorsanız işe yarar. Eğer müşteriler karışıyor, geri dönüşler unutuluyor veya randevuları manuel takip ediyorsanız mantıklı olur."
        if "sosyal medya" in lowered:
            return "Sosyal medya yönetimi, markanızın daha profesyonel görünmesi, düzenli içerik paylaşması ve güven oluşturması için işe yarar. Instagram’da daha görünür olmak ve hesabı düzenli yönetmek istiyorsanız mantıklı olur; direkt müşteri kazanımı hedefleniyorsa reklamla birlikte düşünülmeli."
"""

new_fit_logic = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
) -> str:
    from app.main import sanitize_text, get_config
    catalog = get_config().get('service_catalog', [])
    
    if message_text:
        lowered = sanitize_text(message_text).lower()
        for svc in catalog:
            for kw in svc.get('keywords', []):
                if kw in lowered and 'fit_description' in svc:
                    return svc['fit_description']
"""
code = code.replace(old_fit_logic, new_fit_logic)

# Save
with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("success")
