import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

override_intro = """def apply_ai_first_quality_overrides(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:"""

new_overrides_body = override_intro + """
    decision["reply_text"] = cleanup_ai_first_reply_text(decision.get("reply_text"))

    if is_user_correction_message(message_text) and detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
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
            
    if is_assistant_identity_question(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        return decision
        
    if is_ping_or_attention_message(message_text):
        decision["reply_text"] = "Buradayım, yazabilirsiniz."
        decision["intent"] = "ping_or_attention"
        decision["booking_intent"] = False
        return decision
        
    if is_company_capability_question(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        return decision
        
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        return decision
        
    if is_referral_intent_message(message_text):
        decision["reply_text"] = build_referral_intent_reply()
        decision["intent"] = "referral_intent"
        decision["booking_intent"] = False
        return decision
        
    # TERM CLARIFICATION HERE:
    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
        decision["intent"] = "service_term_clarification"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = "Ön görüşmede işletmenizin hedefini, mevcut durumunu, beklentinizi ve size en uygun olan kapsam/paketi netleştiriyoruz. Uygun görürseniz sonrasında projeyi başlatıyoruz."
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        return decision
        
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
        
    merge_customer_context_memory(message_text, conversation, history)
    
    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, history)
        decision["intent"] = "business_fit"
        decision["booking_intent"] = False
        return decision
        
    if should_use_customer_recommendation_override(message_text, decision, conversation, history):
        if not is_service_term_clarification(message_text):
            decision["reply_text"] = recommendation_engine(conversation, message_text, history)
            decision["intent"] = "business_recommendation"
            decision["booking_intent"] = False
            return decision

"""

parts = text.split(override_intro)
if len(parts) >= 2:
    head = parts[0]
    tail_raw = parts[1]
    
    # Keseceğimiz yer `def should_suppress_ai_booking_collection` veya function sonu.
    next_func_idx = tail_raw.find("def should_suppress_ai_booking_collection")
    if next_func_idx != -1:
        tail = tail_raw[next_func_idx:]
        new_text = head + new_overrides_body + "\n\n" + tail
        with open("app/main.py", "w", encoding="utf-8") as f:
            f.write(new_text)
        print("Success: overridden apply_ai_first_quality_overrides")
    else:
        print("Error context: no should_suppress_ai_booking_collection")
else:
    print("Error context: apply_ai_first_quality_overrides not found")

