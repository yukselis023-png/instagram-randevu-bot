with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

parts = text.split("def apply_ai_first_quality_overrides(")
head = parts[0]
tail_raw = parts[1]
next_func_idx = tail_raw.find("def build_ai_first_emergency_reply(")
tail = tail_raw[next_func_idx:]

new_overrides = """def apply_ai_first_quality_overrides(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decision["reply_text"] = cleanup_ai_first_reply_text(decision.get("reply_text"))
    lowered = sanitize_text(message_text).lower()
    decision_intent = sanitize_text(str(decision.get("intent") or "")).lower()

    # 1. user_correction
    if is_user_correction_message(message_text) and detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # 2. active booking state direct questions
    _active_state = sanitize_text(str(conversation.get("state") or ""))
    if _active_state in ACTIVE_BOOKING_STATES and not is_general_information_request(message_text) and not is_payment_question(message_text) and not is_meeting_method_question(message_text) and "?" not in message_text and not is_invalid_name_attempt(message_text, _active_state):
        if _active_state == "collect_name":
            _svc_display = display_service_name(conversation.get("service")) or "Ön görüşme"
            decision["reply_text"] = f"{_svc_display} kaydını tamamlamak için önce adınızı ve soyadınızı yazar mısınız?"
            decision["intent"] = "booking_collect_name_reask"
            decision["booking_intent"] = True
            decision["missing_fields"] = ["full_name"]
            decision["should_reply"] = True
            return decision
        if _active_state == "collect_phone" and not conversation.get("phone"):
            decision["reply_text"] = "Ön görüşme kaydı için telefon numaranızı paylaşır mısınız?"
            decision["intent"] = "booking_collect_phone_reask"
            decision["booking_intent"] = True
            decision["missing_fields"] = ["phone"]
            decision["should_reply"] = True
            return decision

    if sanitize_text(conversation.get("state") or "") == "collect_name" and is_invalid_name_attempt(message_text, "collect_name"):
        decision["reply_text"] = "Adınızı ve soyadınızı tam olarak yazar mısınız?"
        decision["intent"] = "collect_name_invalid"
        decision["booking_intent"] = True
        decision["missing_fields"] = ["name"]
        return decision

    # 3. completed_booking_followup
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
            decision["missing_fields"] = []
            decision["should_reply"] = True
            return decision
        if is_payment_question(message_text):
            decision["reply_text"] = "Görüşmede ödeme detaylarını konuşuruz; şu an için bir ön ödeme talep etmiyoruz."
            decision["intent"] = "payment_info"
            decision["booking_intent"] = False
            decision["missing_fields"] = []
            decision["should_reply"] = True
            return decision

    # 4. direct_answers
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

    if is_company_capability_question(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_referral_intent_message(message_text):
        decision["reply_text"] = build_referral_intent_reply()
        decision["intent"] = "referral_intent"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_service_term_clarification(message_text):
        decision["reply_text"] = build_service_term_clarification_reply(message_text)
        decision["intent"] = "service_term_clarification"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_assistant_identity_question(message_text):
        if not reply_answers_assistant_identity(decision.get("reply_text")):
            decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = build_contextual_clarification_reply(conversation, message_text)
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_meeting_method_question(message_text) or is_phone_reason_question(message_text):
        if not reply_answers_meeting_method(decision.get("reply_text")):
            decision["reply_text"] = build_contextual_clarification_reply(conversation, message_text)
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_ping_or_attention_message(message_text):
        decision["reply_text"] = "Buradayım, yazabilirsiniz."
        decision["intent"] = "ping_or_attention"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_real_estate_off_topic_question(message_text):
        decision["reply_text"] = build_real_estate_off_topic_reply()
        decision["intent"] = "off_topic"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    direct_service = pick_service(message_text, decision.get("extracted_service") or conversation.get("service"))
    direct_service_meta = match_service_catalog(direct_service, direct_service) if direct_service else None
    
    if is_delivery_time_question(message_text) and sanitize_text(str(decision.get("intent") or "")) in {"fallback_reply", "message_volume", "general_reply", "service_info", "service_overview", "pricing_info"}:
        service_name = conversation.get("service") or pick_service(message_text, decision.get("extracted_service"))
        service_meta = match_service_catalog(service_name, service_name) if service_name else None
        if is_delivery_duration_followup(message_text):
            decision["reply_text"] = build_delivery_duration_followup_reply(service_meta, message_text)
        else:
            decision["reply_text"] = build_delivery_time_reply(service_meta)
        decision["intent"] = "delivery_time"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision

    if is_message_volume_answer(message_text):
        service = conversation.get("service") or "Otomasyon & Yapay Zeka Çözümleri"
        context = {**conversation, "service": service}
        decision["reply_text"] = build_message_volume_reply(message_text, context, history)
        decision["intent"] = "message_volume"
        decision["booking_intent"] = False
        decision["extracted_service"] = service
        decision["missing_fields"] = []
        return decision

    if is_trust_or_scam_question(message_text):
        decision["reply_text"] = build_trust_or_scam_reply()
        decision["intent"] = "reassurance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision

    if is_angry_complaint_message(message_text):
        if not reply_answers_complaint(decision.get("reply_text")):
            decision["reply_text"] = build_angry_complaint_reply()
        decision["intent"] = "complaint"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # 4.a General pricing without memory gets scope answer
    memory = ensure_conversation_memory(conversation)
    has_customer_context = bool(memory.get("customer_sector") or memory.get("customer_subsector") or memory.get("customer_goal"))
    if is_price_question(message_text) and has_customer_context:
        decision["reply_text"] = build_contextual_price_reply(conversation)
        decision["intent"] = "pricing_info"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # 5. service_fit_question
    # !! EXACTLY WHAT THE USER WANTS !!
    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, message_text, history)
        decision["intent"] = "business_fit"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # Update state BEFORE recommendation engine to allow overrides to use fresh identity
    merge_customer_context_memory(message_text, conversation, history)

    # 6. user_business_identity + recommendation
    if should_use_customer_recommendation_override(message_text, decision, conversation, history):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    explicit_business_context = bool(detect_customer_subsector(message_text) or detect_business_sector(message_text))
    if (
        explicit_business_context
        and (is_service_overview_question(message_text) or is_general_information_request(message_text))
        and not is_simple_greeting(message_text)
        and _active_state not in ACTIVE_BOOKING_STATES
    ):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_context_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if (
        (explicit_business_context or is_business_context_intro_message(message_text, history))
        and not is_service_overview_question(message_text)
        and not is_general_information_request(message_text)
        and not is_simple_greeting(message_text)
        and not (match_service_catalog(conversation.get("service"), conversation.get("service")) and match_service_catalog(conversation.get("service"), conversation.get("service")).get("slug") == "web-tasarim")
        and _active_state not in ACTIVE_BOOKING_STATES
    ):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_context_intro"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if direct_service_meta and is_bare_service_interest_message(message_text, direct_service_meta) and not is_assistant_identity_question(message_text) and _active_state not in ACTIVE_BOOKING_STATES:
        decision["reply_text"] = build_short_service_interest_reply(direct_service_meta)
        decision["intent"] = "service_info"
        decision["booking_intent"] = False
        decision["extracted_service"] = str(direct_service_meta.get("display") or direct_service)
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # 7. soft_cta and detail acceptances
    if (
        (is_confirmation_acceptance_message(message_text) or is_detail_continuation_acceptance_message(message_text) or message_shows_booking_intent(message_text, {}))
        and ensure_conversation_memory(conversation).get("pending_offer") != "preconsultation_offer"
        and not recent_outbound_can_start_service_consultation(history, conversation)
        and (
            recent_outbound_offered_more_details(history)
            or recent_outbound_asked_for_detail_continuation(history)
            or recent_outbound_can_accept_automation_details(history, conversation)
        )
    ):
        decision["reply_text"] = build_more_details_acceptance_reply(conversation)
        decision["intent"] = "more_details_acceptance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_explicit_detail_request(message_text) and (
        recent_outbound_offered_consultation(history)
        or recent_outbound_offered_more_details(history)
        or recent_outbound_asked_for_detail_continuation(history)
        or recent_outbound_can_accept_automation_details(history, conversation)
    ):
        decision["reply_text"] = build_more_details_acceptance_reply(conversation)
        decision["intent"] = "more_details_acceptance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    # 8. booking_start
    known_service_name = conversation.get("service") or decision.get("extracted_service") or direct_service
    known_service_meta = match_service_catalog(known_service_name, known_service_name) if known_service_name else None
    
    direct_booking_intent = message_shows_booking_intent(message_text, {}) or (
        bool(direct_service_meta)
        and any(cue in lowered for cue in ["yapalim", "yalalim", "alalim", "alalým", "goruselim", "görüşelim", "planlayalim", "planlayalım", "isterim"])
    )
    
    context_service_name = conversation.get("service") or decision.get("extracted_service")
    context_service_meta = match_service_catalog(context_service_name, context_service_name) if context_service_name else None

    if (
        context_service_meta
        and direct_booking_intent
        and sanitize_text(conversation.get("state") or "") in {"new", "collect_service", "collect_name"}
        and not is_explicit_detail_request(message_text)
    ):
        service_display = str(context_service_meta.get("display") or context_service_name)
        decision["reply_text"] = build_service_consultation_acceptance_reply({"service": service_display})
        decision["intent"] = "service_consultation_acceptance"
        decision["booking_intent"] = True
        decision["extracted_service"] = service_display
        decision["missing_fields"] = ["name"]
        decision["should_reply"] = True
        return decision
        
    if direct_service_meta and direct_booking_intent and sanitize_text(conversation.get("state") or "") in {"new", "collect_service", "collect_name"}:
        service_display = str(direct_service_meta.get("display") or direct_service)
        decision["reply_text"] = build_service_consultation_acceptance_reply({"service": service_display})
        decision["intent"] = "service_consultation_acceptance"
        decision["booking_intent"] = True
        decision["extracted_service"] = service_display
        decision["missing_fields"] = ["name"]
        return decision
        
    if recent_outbound_offered_consultation(history) and is_next_step_prompt(message_text) and not is_explicit_detail_request(message_text):
        inferred_service = infer_recent_service_for_consultation(history, conversation)
        context = {**conversation}
        if inferred_service:
            context["service"] = inferred_service
        decision["reply_text"] = build_service_consultation_acceptance_reply(context)
        decision["intent"] = "service_consultation_acceptance"
        decision["booking_intent"] = True
        if inferred_service:
            decision["extracted_service"] = inferred_service
        decision["missing_fields"] = ["name"]
        return decision
        
    if recent_outbound_can_start_service_consultation(history, conversation) and is_positive_more_details_acceptance(message_text) and not is_explicit_detail_request(message_text):
        inferred_service = infer_recent_service_for_consultation(history, conversation)
        decision["reply_text"] = build_service_consultation_acceptance_reply(conversation)
        decision["intent"] = "service_consultation_acceptance"
        decision["booking_intent"] = True
        if inferred_service:
            decision["extracted_service"] = inferred_service
        decision["missing_fields"] = ["name"]
        return decision
        
    if recent_outbound_can_accept_automation_details(history, conversation) and is_positive_more_details_acceptance(message_text):
        decision["reply_text"] = build_more_details_acceptance_reply(conversation)
        decision["intent"] = "more_details_acceptance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
        
    if is_positive_more_details_acceptance(message_text):
        inferred_service = infer_recent_service_for_consultation(history, conversation)
        if inferred_service:
            decision["reply_text"] = build_service_consultation_acceptance_reply({"service": inferred_service})
            decision["intent"] = "service_consultation_acceptance"
            decision["booking_intent"] = True
            decision["extracted_service"] = inferred_service
            decision["missing_fields"] = ["name"]
            return decision

    # 9. Fallbacks
    if direct_service_meta and reply_asks_service_after_service_known(decision.get("reply_text")):
        service_display = str(direct_service_meta.get("display") or direct_service)
        decision["reply_text"] = build_ai_first_service_information_reply(direct_service_meta, conversation)
        decision["intent"] = "service_info"
        decision["booking_intent"] = False
        decision["extracted_service"] = service_display
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if direct_service_meta and is_service_information_request(message_text, direct_service_meta):
        service_display = str(direct_service_meta.get("display") or direct_service)
        if is_low_quality_ai_first_reply(decision.get("reply_text")) or decision_intent in {"fallback_reply", "general_reply"}:
            decision["reply_text"] = build_ai_first_service_information_reply(direct_service_meta, conversation)
        decision["intent"] = "service_info"
        decision["booking_intent"] = False
        decision["extracted_service"] = service_display
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_general_information_request(message_text) and (
        is_low_quality_ai_first_reply(decision.get("reply_text"))
        or decision_intent in {"fallback_reply", "general_reply"}
        or not reply_mentions_service_context(decision.get("reply_text"))
    ):
        detail_keyword_match = any(keyword in lowered for keyword in DETAIL_KEYWORDS)
        decision["reply_text"] = build_detailed_services_overview_reply() if detail_keyword_match else build_services_overview_reply()
        decision["intent"] = "detailed_service_overview" if detail_keyword_match else "service_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_service_overview_question(message_text):
        detail_keyword_match = any(keyword in lowered for keyword in DETAIL_KEYWORDS)
        if detail_keyword_match:
            decision["reply_text"] = build_detailed_services_overview_reply()
            decision["intent"] = "detailed_service_overview"
        else:
            decision["reply_text"] = build_services_overview_reply()
            decision["intent"] = "service_overview"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
        
    if is_good_wishes_message(message_text) and len(sanitize_text(message_text).split()) <= 4:
        if is_simple_greeting(message_text):
            decision["reply_text"] = "Merhaba, teşekkür ederiz. Hangi hizmetimizle ilgili detay istersiniz?"
        else:
            decision["reply_text"] = build_good_wishes_reply()
        decision["intent"] = "greeting"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
        
    if ("aleykum" in lowered or "aleyküm" in lowered) and len(sanitize_text(message_text).split()) <= 4:
        decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Hangi alanda işletmenizi geliştirmek istersiniz?"
        decision["intent"] = "greeting"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision

    if is_low_quality_ai_first_reply(decision.get("reply_text")):
        decision["reply_text"] = build_ai_first_emergency_reply(message_text, conversation)
        decision["intent"] = "recovered_low_quality_reply"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    return decision
"""

new_text = head + new_overrides + "\n\n" + tail
with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(new_text)

print("done")
