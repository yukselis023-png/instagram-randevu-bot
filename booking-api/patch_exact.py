with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. is_service_term_clarification
old_cl = """def is_service_term_clarification(text: str) -> bool:
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
        
new_cl = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey", "aynı şey", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir", "ne işe yarar", "ne ise yarar"]
        if not any(t in lowered for t in triggers):
            return False
            
        catalog = get_config().get("service_catalog", [])
        for svc in catalog:
            for kw in svc.get("keywords", []):
                if kw in lowered:
                    return True
        return False
    except:
        return False"""
text = text.replace(old_cl, new_cl)

# 2. build_service_term_clarification_reply
old_re = """def build_service_term_clarification_reply(text: str) -> str:
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
        return f"Bahsettiğiniz konu {get_config().get('business_name', '')} hizmetleri kapsamındadır. Detaylı teknik bilgiyi uzman ekibimizle görüşerek alabilirsiniz."
    except:
        return "Detaylı bilgiyi ön görüşmemizde birlikte değerlendirebiliriz."
"""
text = text.replace(old_re, new_re)

# 3. is_business_fit_question
old_is_fit = """def is_business_fit_question(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["uygun mu", "uyar mi", "uyar mı", "isime yarar", "işime yarar", "faydali olur", "faydalı olur", "mantikli mi", "mantıklı mı"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing", "web", "sosyal medya", "reklam", "performans"]
        return any(term in lowered for term in terms)
    except:
        return False"""

new_is_fit = """def is_business_fit_question(text: str) -> bool:
    try:
        from app.main import sanitize_text, get_config
        lowered = sanitize_text(text).lower()
        triggers = ["uygun mu", "uyar mi", "uyar mı", "isime yarar", "işime yarar", "faydali olur", "faydalı olur", "mantikli mi", "mantıklı mı"]
        if not any(t in lowered for t in triggers):
            return False
            
        catalog = get_config().get("service_catalog", [])
        for svc in catalog:
            for kw in svc.get("keywords", []):
                if kw in lowered:
                    return True
        return False
    except:
        return False"""
text = text.replace(old_is_fit, new_is_fit)

# 4. build_business_fit_reply
old_fit_re = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
    history: list[dict[str, Any]] | None = None,
) -> str:
    from app.main import sanitize_text
    
    # Generic service intent extraction
    if message_text:
        lowered = sanitize_text(message_text).lower()
        if "crm" in lowered:
            return "CRM, gelen müşteri taleplerini, randevuları ve takip süreçlerini düzenlemek istiyorsanız işe yarar. Eğer müşteriler karışıyor, geri dönüşler unutuluyor veya randevuları manuel takip ediyorsanız mantıklı olur."
        if "sosyal medya" in lowered:
            return "Sosyal medya yönetimi, markanızın daha profesyonel görünmesi, düzenli içerik paylaşması ve güven oluşturması için işe yarar. Instagram’da daha görünür olmak ve hesabı düzenli yönetmek istiyorsanız mantıklı olur; direkt müşteri kazanımı hedefleniyorsa reklamla birlikte düşünülmeli."
        if "otomasyon" in lowered or "bot" in lowered:
            return "Otomasyon, gelen mesajları anında yanıtlayan ve sizi bekletmeden görüşme planlayan DOEL AI sistemimizi sisteme entegre ediyoruz. Özellikle randevularınız karışıyor, dönüşler gecikiyorsa inanılmaz bir zaman kazandırır."
        elif "landing" in lowered or "web tasarim" in lowered or "web sitesi" in lowered or "website" in lowered or "web" in lowered:
            return "İnternette profesyonel bir vitrin oluşturmak, müşterilerinize güven vermek ve hizmetlerinizi detaylı olarak sunmak istiyorsanız tam markanıza göredir."
        elif "reklam" in lowered or "performans" in lowered:
            return "Yeni müşteriler bulmak ve satışlarınızı ölçeklemek ana hedefinizse, reklam sizin için en kısa yoldur."

    return "Lokal işletmelerde genelde ilk olarak landing page (web) yapıp trafik (reklam) çekmekle başlanır; talep arttıkça yapay zeka otomasyonu gibi gelişmiş sistemler kurulur." """

new_fit_re = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
    history: list[dict[str, Any]] | None = None,
) -> str:
    from app.main import sanitize_text, get_config
    catalog = get_config().get("service_catalog", [])
    if message_text:
        lowered = sanitize_text(message_text).lower()
        for svc in catalog:
            for kw in svc.get("keywords", []):
                if kw in lowered and "fit_description" in svc:
                    return svc["fit_description"]
    return f"{get_config().get('business_name', '')} hizmetleri genel olarak hedeflerinize uygun olabilir. Detayları kısa bir görüşmede netleştirebiliriz." """
text = text.replace(old_fit_re, new_fit_re)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
