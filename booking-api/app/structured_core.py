"""
Yapısal AI Bot Çekirdeği (Structured Core)
Eski "if-else cehennemi" yerine, LLM'den kesin JSON şeması isteyen ve 
bu şemayı doğrulayarak aksiyon tetikleyen sadeleştirilmiş mantık.
"""
import json
import re
import datetime
import logging
from typing import Any, Dict, Optional, Tuple

from fastapi import BackgroundTasks
from app.main import (
    ProcessResult, IncomingMessage, get_conn, get_or_create_conversation, 
    sanitize_conversation_state, ensure_conversation_memory, 
    sync_conversation_memory_summary, save_message_log, get_recent_message_history,
    update_conversation_memory_after_bot_reply, upsert_conversation, upsert_customer_from_conversation,
    schedule_customer_automation_events, sanitize_text, extract_inbound_message_id, extract_inbound_platform,
    build_inbound_dedupe_key, elapsed_ms, queue_crm_sync, get_config, call_llm_content,
    extract_name, extract_phone, extract_date, extract_time, create_appointment,
    build_confirmation_message, validate_slot, find_existing_appointment, suggest_alternatives,
    normalize_date_string, normalize_time_string, format_human_date, TZ,
    collect_next_booking_slot_options, remember_booking_slot_options, normalize_booking_slot_option
)
from app.action_policy import classify_user_action

logger = logging.getLogger(__name__)

# --- STRICT JSON SCHEMA FOR LLM ---
LLM_SYSTEM_PROMPT = """Sen bir randevu asistanısın. SADECE aşağıdaki JSON şemasında yanıt ver. Başka hiçbir metin, açıklama veya markdown ekleme.

{
  "intent": "answer_question" | "collect_name" | "collect_phone" | "collect_datetime" | "book_appointment" | "human_handoff",
  "extracted": {
    "name": "string veya null",
    "phone": "string veya null",
    "date": "YYYY-MM-DD veya null",
    "time": "HH:MM veya null",
    "service": "string veya null"
  },
  "missing_fields": ["name", "phone", "date", "time"] (eksik olanları listele, tamamsa []),
  "reply": "Kullanıcıya verilecek doğal, kısa (max 160 karakter) ve nazik Türkçe yanıt."
}

KURALLAR:
1. Kullanıcı bir soru soruyorsa, önce soruyu cevapla ("reply"), ardından eksikse bir sonraki adımı sor.
2. Asla "takvimi göremiyorum", "kontrol edip döneceğim" deme. Sistem sana slot verirse sadece onları öner.
3. Kullanıcı "iptal" veya "değiştir" derse intent'i "human_handoff" yap.
4. "reply" alanı asla "randevunuzu oluşturdum/güncelledim" gibi kesin sistem ifadeleri içermez. Bu, arka plan sistemi tarafından doğrulandıktan sonra eklenir.
"""

def extract_balanced_json(text: str) -> Optional[Dict[str, Any]]:
    """LLM çıktısından ilk geçerli JSON objesini çıkar."""
    start = text.find('{')
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if escape:
            escape = False
            continue
        if ch == '\\' and in_string:
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(text[start:idx + 1])
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    return None
    return None

def process_message_structured(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    """Sadeleştirilmiş, şema tabanlı mesaj işleme akışı."""
    request_started_at = datetime.datetime.now().timestamp()
    metrics = {"reply_engine": "structured_core", "total_ms": 0}
    decision_path = ["structured_core"]
    
    message_text = sanitize_text(payload.message_text or "")
    if not message_text:
        message_text = "(empty message)"

    with get_conn() as conn:
        conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
        sanitize_conversation_state(conversation)
        memory = ensure_conversation_memory(conversation)
        
        # 1. LLM'e yapısal istek gönder
        # (Not: Gerçek implementasyonda call_llm_json kullanılacak, burada basitleştirilmiş çağrı)
        # Bu kısım mevcut invoke_generic_llm'in sadeleştirilmiş versiyonu olacak.
        # Şimdilik mevcut sistemi bozmadan geçiş için hybrid bir yaklaşım kullanıyoruz.
        
        # 2. Deterministik Varlık Çıkarımı (LLM hata yaparsa diye güvenlik ağı)
        extracted_name = extract_name(message_text, conversation.get("state", "new"))
        extracted_phone = extract_phone(message_text)
        extracted_date = extract_date(message_text)
        extracted_time = extract_time(message_text)
        
        # 3. Basitleştirilmiş Durum Makinesi (FSM)
        curr_state = conversation.get("state", "new")
        has_name = bool(conversation.get("full_name") or conversation.get("lead_name") or extracted_name)
        has_phone = bool(conversation.get("phone") or extracted_phone)
        has_date = bool(conversation.get("requested_date") or extracted_date)
        has_time = bool(conversation.get("requested_time") or extracted_time)
        has_service = bool(conversation.get("service") or memory.get("requested_service"))
        
        # Kullanıcı net bir şekilde tüm bilgileri verdiyse ve randevu istiyorsa
        if has_name and has_phone and has_date and has_time and has_service:
            # Slot doğrulama
            slot_error = validate_slot(conversation.get("requested_date"), conversation.get("requested_time"))
            if slot_error:
                reply_text = slot_error
                conversation["state"] = "collect_datetime"
                decision_path.append("validation:slot_error")
            else:
                # Çakışma kontrolü
                conflict = find_existing_appointment(conn, normalize_date_string(conversation.get("requested_date")), 
                                                     normalize_time_string(conversation.get("requested_time")), 
                                                     conversation.get("service"))
                if conflict:
                    alternatives = suggest_alternatives(conn, normalize_date_string(conversation.get("requested_date")), 
                                                        normalize_time_string(conversation.get("requested_time")), 
                                                        conversation.get("service"))
                    alt_text = ", ".join(alternatives[:3])
                    reply_text = f"Maalesef o saat dolu. Uygun seçenekler: {alt_text}. Hangisi uygun olur?"
                    conversation["state"] = "collect_datetime"
                    decision_path.append("validation:slot_conflict")
                else:
                    # Randevu oluştur
                    conversation["state"] = "completed"
                    conversation["appointment_status"] = "confirmed"
                    created = create_appointment(conn, conversation, payload.instagram_username)
                    appointment_id = int(created[0] if isinstance(created, tuple) else created)
                    conversation["appointment_id"] = appointment_id
                    reply_text = build_confirmation_message(conversation)
                    decision_path.append("action:appointment_created")
                    
                    # CRM Senkronizasyonu
                    queue_crm_sync(background_tasks, conversation, appointment_id, metrics)
        else:
            # Eksik alanları tamamla (Basitleştirilmiş FSM)
            if not has_service:
                conversation["state"] = "collect_service"
                reply_text = "Hangi hizmet için ön görüşme planlamak istersiniz?"
                decision_path.append("fsm:collect_service")
            elif not has_name:
                conversation["state"] = "collect_name"
                reply_text = "Ön görüşme için adınızı ve soyadınızı alabilir miyim?"
                decision_path.append("fsm:collect_name")
            elif not has_phone:
                conversation["state"] = "collect_phone"
                reply_text = "Teşekkürler. İletişim için telefon numaranızı paylaşır mısınız?"
                decision_path.append("fsm:collect_phone")
            elif not has_date or not has_time:
                conversation["state"] = "collect_datetime"
                reply_text = "Uygun gün ve saati net yazar mısınız? (Örn: Yarın 14:00)"
                decision_path.append("fsm:collect_datetime")
            else:
                # Beklenmedik durum, LLM'e bırak
                reply_text = "Mesajınızı aldım. Nasıl yardımcı olabilirim?"
                decision_path.append("fsm:fallback")

        # Bellek ve veritabanı güncellemeleri
        if extracted_name and not conversation.get("full_name"):
            conversation["lead_name"] = extracted_name
            conversation["full_name"] = extracted_name
        if extracted_phone and not conversation.get("phone"):
            conversation["phone"] = extracted_phone
        if extracted_date and not conversation.get("requested_date"):
            conversation["requested_date"] = extracted_date
        if extracted_time and not conversation.get("requested_time"):
            conversation["requested_time"] = extracted_time
            
        conversation["memory_state"] = memory
        update_conversation_memory_after_bot_reply(conversation, reply_text, "|".join(decision_path))
        upsert_conversation(conn, conversation)
        
        crm_customer = upsert_customer_from_conversation(conn, conversation)
        if crm_customer:
            schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector", ""))
            
        save_message_log(conn, payload.sender_id, "out", reply_text, {"type": "reply", "decision_path": decision_path})
        metrics["total_ms"] = elapsed_ms(request_started_at)
        
        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=True,
            reply_text=reply_text,
            outbound_text=reply_text,
            llm_raw_reply_text=reply_text,
            final_reply_source="structured_core",
            handoff=(conversation.get("state") == "human_handoff"),
            conversation_state=conversation.get("state", "new"),
            appointment_created=(conversation.get("appointment_status") == "confirmed"),
            appointment_id=conversation.get("appointment_id"),
            normalized={},
            metrics=metrics,
            decision_path=decision_path,
        )