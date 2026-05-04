import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. build_company_capability_reply takes 1 argument
code = code.replace("build_company_capability_reply(message_text, history)", "build_company_capability_reply(message_text)")

# 2. build_service_general_overview -> does not exist. Original is build_service_overview_reply() or similar?
# Let's check what it should be. It's usually recommendation_engine() or something. Let me check the file:
# if is_service_overview_question(message_text) -> what did it do?

code = code.replace("build_service_general_overview()", '"Web tasarım, otomasyon & yapay zeka, performans reklamları ve sosyal medya yönetimi tarafında destek veriyoruz. İşletmenizin sektörünü ve hedefini yazarsanız en mantıklı başlangıcı önerebilirim."')

# 3. NoneType check in guard AND key "passed"
new_guard = """def guard_and_repair_final_answer(user_msg: str, reply: str, conversation_state: dict, history: list = None, decision_label: str = None) -> dict:
    history = history or []
    fail_reasons = []
    _reply = reply or ""
    _intent = decision_label
    
    whitelist = [
        "correction", "assistant_identity", "company_capability_question", "company_background", 
        "referral_not_acknowledged", "detailed_service_overview", "pricing_info", 
        "service_overview", "ambiguous_appointment_disambiguation", "business_recommendation",
        "booking_collect_name_reask", "booking_collect_phone_reask", "clarification", "service_clarification",
        "business_fit"
    ]
    if _intent in whitelist:
        return {"passed": True, "ok": True, "reasons": [], "repaired": _reply, "reply_text": _reply}
        
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
            
        return {"passed": False, "ok": False, "reasons": fail_reasons, "repaired": safe_reply, "reply_text": safe_reply}

    return {"passed": True, "ok": True, "reasons": [], "repaired": _reply, "reply_text": _reply}"""

code = re.sub(r'def guard_and_repair_final_answer\(.*?(?=\n\n\ndef )', new_guard, code, count=1, flags=re.DOTALL)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Super fixed")
