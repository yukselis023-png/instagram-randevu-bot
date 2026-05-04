import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# REPLACE THE ENTIRE apply_ai_first_quality_overrides with STRICT PRIORITY ROUTER
pattern_to_replace = re.search(r"def apply_ai_first_quality_overrides\(.*?return decision", code, re.DOTALL)

strict_router = """def apply_ai_first_quality_overrides(conversation: dict, message_text: str, history: list, llm_data: dict | None, direct_service_meta: dict | None, direct_service: str | None) -> dict:
    decision = {}
    lowered = sanitize_text(message_text).lower()
    _active_state = str(conversation.get("state") or "")
    
    # ==== 0. State Corrections & Booking Lifecycle First ====
    # Handle explicit re-asks inside booking directly to not break the flow.
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

    # ==== 1. user_correction ====
    if match_correction_message(message_text):
        decision["reply_text"] = build_correction_reply()
        decision["intent"] = "correction"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 2. human/person question | 3. bot identity question ====
    if is_assistant_identity_question(message_text) or is_owner_check_message(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 4. company capability question ====
    if detect_company_capability_activity(message_text):
        decision["reply_text"] = build_company_capability_reply(message_text, history)
        decision["intent"] = "company_capability_question"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 5. company info question ====
    if is_company_background_question(message_text):
        decision["reply_text"] = build_company_background_reply()
        decision["intent"] = "company_background"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 6. referral intent ====
    if getattr(globals(), 'is_referral_intent_message', lambda x: False)(message_text):
        decision["reply_text"] = getattr(globals(), 'build_referral_intent_reply', lambda: "Arkadaşınızın önerisi için teşekkür ederiz. DOEL Digital olarak web sitesi, reklam, sosyal medya yönetimi ve otomasyon hizmetleri tarafında destek oluyoruz;")()
        decision["intent"] = "referral_not_acknowledged"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 7. service_term_clarification_question ====
    if is_service_clarification_request(message_text):
        decision["reply_text"] = "Web tasarım ve dijital pazarlama süreçlerinizi iyileştiren, satışlarınızı artıran altyapılar kuruyoruz. Sizin işletmenizin ne tip bir ihtiyacı vardı?"
        decision["intent"] = "service_clarification"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 8. consultation explanation question ====
    if is_meeting_clarification_question(message_text):
        decision["reply_text"] = build_meeting_clarification_reply()
        decision["intent"] = "clarification"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 9. service list follow-up (Detailed Services / Looping prevention) ====
    if hasattr(globals(), 'is_detailed_service_question') and globals()['is_detailed_service_question'](message_text, history):
        decision["reply_text"] = globals()['build_detailed_service_reply']()
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

    # ==== 10. price question ====
    if is_price_question(message_text):
        decision["reply_text"] = build_contextual_price_reply(conversation)
        decision["intent"] = "pricing_info"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 11. fit/suitability question ====
    if is_business_fit_question(message_text):
        decision["reply_text"] = build_business_fit_reply(conversation, message_text, history)
        decision["intent"] = "business_fit"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 12. customer acquisition goal ====
    goal = getattr(globals(), 'detect_customer_goal', lambda x, y: None)(message_text, history)
    if goal == "more_bookings":
        # Disambiguate if ambiguous appointment instead!
        if hasattr(globals(), 'is_ambiguous_appointment_question') and globals()['is_ambiguous_appointment_question'](message_text):
            decision["reply_text"] = globals()['build_ambiguous_appointment_reply']()
            decision["intent"] = "ambiguous_appointment_disambiguation"
            decision["booking_intent"] = False
            decision["should_reply"] = True
            return decision

        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # Disambiguate anything that looks like appointment confusion BEFORE recommendation:
    if hasattr(globals(), 'is_ambiguous_appointment_question') and globals()['is_ambiguous_appointment_question'](message_text):
        decision["reply_text"] = globals()['build_ambiguous_appointment_reply']()
        decision["intent"] = "ambiguous_appointment_disambiguation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision

    # ==== 13. user business identity + recommendation ====
    explicit_business_context = bool(detect_customer_subsector(message_text) or detect_business_sector(message_text))
    if _active_state not in ACTIVE_BOOKING_STATES and (explicit_business_context or is_business_context_intro_message(message_text, history)):
        decision["reply_text"] = recommendation_engine(conversation, message_text, history)
        decision["intent"] = "business_recommendation"
        decision["booking_intent"] = False
        decision["should_reply"] = True
        return decision
        
    # fallback to LLM default response
    return decision"""

if pattern_to_replace:
    code = code[:pattern_to_replace.start()] + strict_router + code[pattern_to_replace.end():]
    
# Now fix guard_and_repair_final_answer to NEVER send wrong ai replies
guard_target_func = re.search(r"def guard_and_repair_final_answer\(.*?return {", code, re.DOTALL)
if guard_target_func:
    new_guard_func = """def guard_and_repair_final_answer(history: list, latest_decision: dict, user_msg: str, conversation_state: dict) -> dict:
    fail_reasons = []
    _reply = latest_decision.get("reply_text") or ""
    
    # Check deterministic first!
    if latest_decision.get("intent") in ["correction", "assistant_identity"
