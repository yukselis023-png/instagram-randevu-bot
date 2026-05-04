def rewrite():
    try:
        path = r"C:\Users\oyunc\Desktop\instagram-randevu-bot\booking-api\app\main.py"
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()

        router_start = "def apply_ai_first_quality_overrides"
        router_end = "    return decision\n\n\ndef should_replace_collection_reply_with_clarification"

        guard_start = "def guard_and_repair_final_answer"
        guard_end = '    return {"ok": True, "reasons": [], "repaired": _reply}\n\n\ndef build_assistant_identity_reply'

        # First extract the chunks to ensure we don't break
        pre_router = code.split(router_start)[0]
        post_router = router_end + code.split(router_end)[1]

        # For guard, we will split the post_router part
        pre_guard = post_router.split(guard_start)[0]
        post_guard = guard_end + post_router.split(guard_end)[1]

        new_overrides = """def apply_ai_first_quality_overrides(conversation: dict, message_text: str, history: list, llm_data: dict | None, direct_service_meta: dict | None, direct_service: str | None) -> dict:
    decision = {}
    _active_state = str(conversation.get("state") or "")
    
    if _active_state == "collect_name" and not conversation.get("full_name"):
        decision["reply_text"] = "Ön görüşme kaydını tamamlamak için adınızı ve soyadınızı yazar mısınız?"
        decision["intent"] = "booking_collect_name_reask"
        decision["booking_intent"] = True
        return decision
    if _active_state == "collect_phone" and not conversation.get("phone"):
        decision["reply_text"] = "Ön görüşme kaydı için telefon numaranızı paylaşır mısınız?"
        decision["intent"] = "booking_collect_phone_reask"
        decision["booking_intent"] = True
        return decision

    if match_correction_message(message_text):
        decision["reply_text"] = build_correction_reply()
        decision["intent"] = "correction"
        return decision

    if is_assistant_identity_question(message_text) or is_owner_check_message(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        return decision

    if detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text, history)
        decision["intent"] = "company_capability_question"
        return decision

    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        return decision

    if "önerdiği" in sanitize_text(message_text).lower() or "arkadaşım" in sanitize_text(message_text).lower() or "referans" in sanitize_text(message_text).lower():
        decision["reply_text"] = "Arkadaşınızın önerisi için teşekkür ederiz. DOEL Digital olarak web tasarım, reklam, sosyal medya yönetimi ve otomasyon hizmetleri tarafında destek oluyoruz."
        decision["intent"] = "referral_not_acknowledged"
        return decision

    if is_service_clarification_request(message_text):
        decision["reply_text"] = "Web tasarım ve dijital pazarlama süreçlerinizi iyileştiren, satışlarınızı artıran altyapılar kuruyoruz. Sizin işletmenizin ne tip bir ihtiyacı vardı?"
        decision["intent"] = "service_clarification"
        return decision

    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = build_meeting_clarification_reply()
        decision["intent"] = "clarification"
        return decision

    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        return decision
        
    if is_service_overview_question(message_text):
        decision["reply_text"] = build_service_general_overview()
        decision["intent"] = "service_overview"
        return decision

    if is_price_question(message_text):
        decision["reply_text"] = build_contextual_price_reply(conversation)
        decision["intent"] = "pricing_info"
        return decision

    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, message_text, history)
        decision["intent"] = "business_fit"
        return decision

    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        return decision

    ca_goal = detect_customer_goal(message_text, history)
    if ca_goal == "more_bookings":
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        return decision

    explicit_bus = bool(detect_customer_subsector(message_text) or detect_business_sector(message_text))
    if _active_state not in ACTIVE_BOOKING_STATES and (explicit_bus or is_business_context_intro_message(message_text, history)):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        return decision
        
"""

        new_guard = """def guard_and_repair_final_answer(history: list, latest_decision: dict, user_msg: str, conversation_state: dict) -> dict:
    fail_reasons = []
    _reply = latest_decision.get("reply_text") or ""
    _intent = latest_decision.get("intent")
    
    whitelist = [
        "correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation",
        "booking_collect_name_reask", "booking_collect_phone_reask", "clarification", "service_clarification",
        "business_fit"
    ]
    if _intent in whitelist:
        return {"ok": True, "reasons": [], "repaired": _reply}
        
    if "Size nasıl yardımcı olabiliriz" in _reply and "Size nasıl" in get_last_outbound_text(history):
        fail_reasons.append("repeated_greeting")
        
    if "sektörünü" in _reply.lower() and (detect_customer_subsector(user_msg) or detect_business_sector(user_msg)):
        fail_reasons.append("ask_sector_when_already_provided")
        
    if "dolu görünüyor" in _reply and "dolu görünüyor" in get_last_outbound_text(history):
        fail_reasons.append("repeated_time_block")
        
    if len(_reply) > 700:
        fail_reasons.append("too_long")
        
    if fail_reasons:
        if "repeated_time_block" in fail_reasons:
            safe_reply = "Seçtiğiniz saat doluydu. Lütfen farklı saat önerebilir misiniz?"
        elif "too_long" in fail_reasons:
            safe_reply = "Detayları ön görüşmemizde birlikte değerlendirmek daha sağlıklı olacaktır. Ne zaman planlayalım?"
        elif "repeated_greeting" in fail_reasons:
            safe_reply = "İşletmeniz için hangi alanda destek arıyorsunuz?"
        else:
            safe_reply = "Bu konuyu detaylandırmak için iletişimi başlatabilir misiniz?"
            
        return {"ok": False, "reasons": fail_reasons, "repaired": safe_reply}

"""

        final_code = pre_router + new_overrides + pre_guard + new_guard + post_guard
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(final_code)

        print("Split patched successfully!")

    except Exception as e:
        print(f"Error: {e}")

rewrite()