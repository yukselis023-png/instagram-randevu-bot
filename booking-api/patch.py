import sys

def rewrite():
    try:
        path = r"C:\Users\oyunc\Desktop\instagram-randevu-bot\booking-api\app\main.py"
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
        start_idx = -1
        end_idx = -1
        for i, line in enumerate(lines):
            if line.startswith("def apply_ai_first_quality_overrides("):
                start_idx = i
            if start_idx != -1 and line.strip() == "return decision" and i > start_idx + 10:
                if i > end_idx:
                    end_idx = i
                    
        guard_start = -1
        guard_end = -1
        for i, line in enumerate(lines):
            if line.startswith("def guard_and_repair_final_answer("):
                guard_start = i
            if guard_start != -1 and line.startswith("def build_assistant_identity_reply"):
                guard_end = i - 1
                break

        if start_idx == -1 or guard_start == -1:
            print("Failed to find bounds.")
            return
            
        new_overrides = """def apply_ai_first_quality_overrides(conversation: dict, message_text: str, history: list, llm_data: dict | None, direct_service_meta: dict | None, direct_service: str | None) -> dict:
    decision = {}
    _active_state = str(conversation.get("state") or "")
    
    # 0. Bookings Collection Constraints
    if _active_state == "collect_name" and not conversation.get("full_name"):
        decision["reply_text"] = "Ön görüşme kaydını tamamlamak için adınızı ve soyadınızı yazar mısınız?"
        decision["intent"] = "booking_collect_name_reask"
        decision["booking_intent"] = True
        decision["should_reply"] = True
        return decision
    if _active_state == "collect_phone" and not conversation.get("phone"):
        decision["reply_text"] = "Ön görüşme kaydı için telefon numaranızı paylaşır mısınız?"
        decision["intent"] = "booking_collect_phone_reask"
        decision["booking_intent"] = True
        decision["should_reply"] = True
        return decision

    # 1. user_correction
    if match_correction_message(message_text):
        decision["reply_text"] = build_correction_reply()
        decision["intent"] = "correction"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 2. bot identity question & 3. human/person question
    if is_assistant_identity_question(message_text) or is_owner_check_message(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 4. company capability question
    if detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text, history)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 5. company info question
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 6. referral intent
    if is_referral_intent_message(message_text):
        decision["reply_text"] = build_referral_intent_reply()
        decision["intent"] = "referral_not_acknowledged"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 7. service_term_clarification_question
    if is_service_clarification_request(message_text):
        decision["reply_text"] = "Web tasarım ve dijital pazarlama süreçlerinizi iyileştiren, satışlarınızı artıran altyapılar kuruyoruz. Sizin işletmenizin ne tip bir ihtiyacı vardı?"
        decision["intent"] = "service_clarification"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 8. consultation explanation question
    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = build_meeting_clarification_reply()
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 9. service list follow-up (Looping prevention before overview)
    if is_detailed_service_question(message_text, history):
        decision["reply_text"] = build_detailed_service_reply()
        decision["intent"] = "detailed_service_overview"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision
        
    if is_service_overview_question(message_text):
        decision["reply_text"] = build_service_general_overview()
        decision["intent"] = "service_overview"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 10. price question
    if is_price_question(message_text):
        decision["reply_text"] = build_contextual_price_reply(conversation)
        decision["intent"] = "pricing_info"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # 11. fit/suitability question
    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, message_text, history)
        decision["intent"] = "business_fit"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==========================================
    # AMBIGUITY DISCOVERY BEFORE RECOMMENDATION!
    if is_ambiguous_appointment_question(message_text):
        decision["reply_text"] = build_ambiguous_appointment_reply()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision
    # ==========================================

    # 12. customer acquisition goal
    ca_goal = detect_customer_goal(message_text, history)
    if ca_goal == "more_bookings":
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision
        
    # 13. user business identity + recommendation
    explicit_bus = bool(detect_customer_subsector(message_text) or detect_business_sector(message_text))
    if _active_state not in ACTIVE_BOOKING_STATES and (explicit_bus or is_business_context_intro_message(message_text, history)):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision
        
    return decision
"""

        new_guard = """def guard_and_repair_final_answer(history: list, latest_decision: dict, user_msg: str, conversation_state: dict) -> dict:
    fail_reasons = []
    _reply = latest_decision.get("reply_text") or ""
    _intent = latest_decision.get("intent")
    
    # 1) VALIDATED INTENTS - THESE BYPASS GUARD (Strict Execution path)
    whitelist = [
        "correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation",
        "booking_collect_name_reask", "booking_collect_phone_reask", "clarification", "service_clarification",
        "business_fit"
    ]
    if _intent in whitelist:
        return {"ok": True, "reasons": [], "repaired": _reply}
        
    # 2) GUARDS FOR EVERYTHING ELSE (LLM Generative logic or uncaught edges)
    if "Size nasıl yardımcı olabiliriz" in _reply and "Size nasıl" in get_last_outbound_text(history):
        fail_reasons.append("repeated_greeting")
        
    if "sektörünü" in _reply.lower() and (detect_customer_subsector(user_msg) or detect_business_sector(user_msg)):
        fail_reasons.append("ask_sector_when_already_provided")
        
    if "dolu görünüyor" in _reply and "dolu görünüyor" in get_last_outbound_text(history):
        fail_reasons.append("repeated_time_block")
        
    if len(_reply) > 700:
        fail_reasons.append("too_long")
        
    # IF GUARD FAILS -> NO AI OUTPUT! PURE DETERMINISTIC REPAIR
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

    # Pass
    return {"ok": True, "reasons": [], "repaired": _reply}
"""

        final_lines = lines[:start_idx] + [new_overrides] + lines[end_idx+1:guard_start] + [new_guard] + lines[guard_end:]
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(final_lines)
        print("Written successfully!")
    except Exception as e:
        print(f"Error: {e}")

rewrite()
