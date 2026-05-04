import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. ADD NEW HELPER FUNCTIONS FOR TERM CLARIFICATION
helpers = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey mi", "aynı şey mi", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing page", "web tasarim", "web sitesi", "website", 
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
code = code.replace("def is_service_overview_question(", helpers)


# 2. REWRITE applies_ai_first_quality_overrides STRICTLY
target_func = """def apply_ai_first_quality_overrides(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decision["reply_text"] = cleanup_ai_first_reply_text(decision.get("reply_text"))"""

new_overrides_body = target_func + """

    # STRICT ROUTER PRIORITY (1-15)
    
    # 1. User Corrections & Identity
    if is_user_correction_message(message_text) and detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    # 2. Active Booking / Completed Followups
    if is_completed_booking_closeout_message(message_text, conversation):
        decision["reply_text"] = "Rica ederiz, görüşme saatinde bekliyoruz."
        decision["intent"] = "closing"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    _conv_state = sanitize_text(str(conversation.get("state") or ""))
    _apt_status = sanitize_text(str(conversation.get("appointment_status") or ""))
    if _apt_status == "confirmed" or _conv_state == "completed":
        _apt_date = conversation.get("requested_date") or conversation.get("appointment_date", "")
        _apt_time = conversation.get("requested_time") or conversation.get("appointment_time", "")
        if (extract_date(message_text) or extract_time(message_text) or has_date_cue(message_text)) and not wants_new_booking_after_confirmation(message_text):
            teyit = f"Randevunuz kayıtlı: {_apt_date} saat {_apt_time}. Görüşmede bekliyoruz."
            decision["reply_text"] = teyit
            decision["intent"] = "booking_confirmed_ack"
            decision["booking_intent"] = False
            return decision
        if is_payment_question(message_text):
            decision["reply_text"] = "Görüşmede ödeme detaylarını konuşuruz; şu an için bir ön ödeme talep etmiyoruz."
            decision["intent"] = "payment_info"
            decision["booking_intent"] = False
            return decision
            
    # 3. Assistant Identity
    if is_assistant_identity_question(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        return decision
        
    # 4. Ping/Attention
    if is_ping_or_attention_message(message_text):
        decision["reply_text"] = "Buradayım, yazabilirsiniz."
        decision["intent"] = "ping_or_attention"
        decision["booking_intent"] = False
        return decision
        
    # 5. Company Capability
    if is_company_capability_question(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        return decision
        
    # 6. Company Background
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        return decision
        
    # 7. Referral Intent
    if is_referral_intent_message(message_text):
        decision["reply_text"] = build_referral_intent_reply()
        decision["intent"] = "referral_intent"
        decision["booking_intent"] = False
        return decision
        
    # 8. Service TERM Clarification (NEW - CRITICAL)
    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
        decision["intent"] = "service_term_clarification"
        decision["booking_intent"] = False
        return decision
        
    # 9. Meeting Clarification
    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = "Ön görüşmede işletmenizin hedefini, mevcut durumunu, beklentinizi ve size en uygun olan kapsam/paketi netleştiriyoruz. Uygun görürseniz sonrasında projeyi başlatıyoruz."
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        return decision
        
    # 10. Service Overviews
    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        decision["booking_intent"] = False
        return decision
        
    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        return decision
        
    # Merge customer memory for remaining steps
    merge_customer_context_memory(message_text, conversation, history)
    
    # 11. Business Fit
    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, history)
        decision["intent"] = "business_fit"
        decision["booking_intent"] = False
        return decision
        
    # 12. Recommendation
    if should_use_customer_recommendation_override(message_text, decision,
