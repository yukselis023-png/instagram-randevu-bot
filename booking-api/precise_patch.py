import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

helpers = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey mi", "aynı şey mi", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing", "web tasarim", "web sitesi", "website", 
                 "sosyal medya", "reklam", "performans"]
        return any(term in lowered for term in terms)
    except:
        return False

def build_service_term_clarification_reply(text: str) -> str:
    from app.main import sanitize_text
    lowered = sanitize_text(text).lower()
    
    if "otomasyon" in lowered:
        return "Otomasyon, tekrar eden işleri sistemin otomatik yapmasıdır. Örneğin gelen mesajlara yanıt verme, randevu toplama ve müşteri takibini düzenleme gibi süreçleri kolaylaştırır."
    elif "crm" in lowered:
        return "CRM, müşteri takip sistemi demektir. Gelen müşterileri, randevuları, konuşmaları ve süreçleri daha düzenli yönetmenizi sağlar."
    elif "landing" in lowered or "landing page" in lowered:
        return "Landing page, reklamdan gelen kişiyi tek bir hedefe yönlendiren özel sayfadır. Genelde WhatsApp'a yazma, form doldurma veya randevu alma gibi dönüşümler için kullanılır."
    elif "web" in lowered and ("tasarim" in lowered or "site" in lowered):
        return "Evet, çoğu zaman aynı anlamda kullanılır. Web tasarım, web sitesinin görünüm, yapı ve kullanıcı deneyimi tarafını ifade eder."
    elif "sosyal" in lowered or "medya" in lowered:
        return "Sosyal medya yönetimi, işletmenizin Instagram/Facebook gibi hesaplarında içerik üretimi, paylaşım düzeni ve marka imajını profesyonelce kurgulamayı kapsar."
    elif "reklam" in lowered:
        return "Performans reklamı, bütçenizi doğrudan potansiyel müşterilere ulaşacak şekilde optimize ettiğimiz ücretli sponsorlu kampanyalardır. Takipçi değil, dönüşüm/satış odaklıdır."
        
    return "Hizmetlerimiz temel olarak dijital görünürlüğünüzü ve müşteri çekme/yönetme süreçlerinizi iyileştirir."

def is_service_overview_question("""

text = text.replace("def is_service_overview_question(", helpers)


injection_target = """    if is_assistant_identity_question(message_text):"""

injection_code = """    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
        decision["intent"] = "service_term_clarification"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_assistant_identity_question(message_text):"""

text = text.replace(injection_target, injection_code)


guard_whitelist_target = """"correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation"
    ]"""

guard_whitelist_replacement = """"correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation", "service_term_clarification"
    ]"""

text = text.replace(guard_whitelist_target, guard_whitelist_replacement)


guard_check_target = """        return {"passed": True, "reason": "whitelisted_intent"}"""

guard_check_replacement = """        return {"passed": True, "reason": "whitelisted_intent"}
        
    if is_service_term_clarification(message_text):
        if any(w in str(reply_text).lower() for w in ["tl", "fiyat", "paket", "reklam kampanyasi", "tutar"]):
            return {"passed": False, "reason": "service_term_clarification"}"""

text = text.replace(guard_check_target, guard_check_replacement)


safe_rep_target = """    elif first["reason"] == "repeated_greeting":
        safe_rep = "İşletmeniz için hangi alanda destek arıyorsunuz?"
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": first["reason"]}"""

safe_rep_replacement = safe_rep_target + """
    elif first["reason"] == "service_term_clarification":
        safe_rep = build_service_term_clarification_reply(message_text)
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": "service_term_clarification"}
"""

text = text.replace(safe_rep_target, safe_rep_replacement)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    tcode = f.read()

tests_append = """
def test_term_clarification_otomasyon(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: '{"reply_text": "Web sitesi tarafinda sik ve guven veren yapi; 12.900 TL", "intent": "service_advice", "booking_intent": False}')
    decision = main.build_ai_first_decision("Otomasyon ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "otomatik yapmasidir" in reply or "tekrar eden isleri" in reply
    assert "12.900" not in reply

def test_term_clarification_crm(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: '{"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}')
    decision = main.build_ai_first_decision("CRM ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "musteri takip sistemi" in reply

def test_term_clarification_landing_page(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: '{"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}')
    decision = main.build_ai_first_decision("Landing page ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "reklamdan gelen" in reply or "ozel sayfa" in reply

def test_term_clarification_web_tasarim(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: '{"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}')
    decision = main.build_ai_first_decision("Web tasarimla web sitesi ayni sey mi?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "ayni anlam" in reply or "evet" in reply

def test_term_clarification_sosyal_medya(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: '{"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}')
    decision = main.build_ai_first_decision("Sosyal medya yonetimi neyi kapsiyor?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "icerik uretimi" in reply or "paylasim duzeni" in reply

def test_term_clarification_forced_bad_ai(monkeypatch):
    from app import main
    conversation = {"state": "new", "memory_state": {}}
    bad_reply = "Web sitesi tarafinda 12.900 TL. Uygun mudur?"
    guarded = main.guard_and_repair_final_answer("Otomasyon ne demek?", bad_reply, conversation, [], decision_label="service_advice")
    reply = main.sanitize_text(guarded["reply_text"]).lower()
    assert guarded["passed"] is True
    assert "otomatik yapmasidir" in reply or "tekrar" in reply
    assert "12.900" not in reply
"""

if "def test_term_clarification_otomasyon" not in tcode:
    with open("tests/test_dm_quality_scenarios.py", "a", encoding="utf-8") as f:
        f.write(tests_append)

print("done")
