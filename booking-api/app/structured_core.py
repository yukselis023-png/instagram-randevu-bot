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
from fastapi import HTTPException
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
  "missing_fields": ["name", "phone", "date", "time"],
  "reply": "Kullanıcıya verilecek doğal, kısa (max 160 karakter) ve nazik Türkçe yanıt."
}

KESİN KURALLAR:
1. SADECE JSON DÖNDÜR: Yanıtın ilk karakteri '{', son karakteri '}' olmalı.
2. KISA YAZ: "reply" alanı 160 karakteri geçmesin. En fazla 2 kısa cümle.
3. SORU CEVAPLA: Kullanıcı bir soru soruyorsa, önce soruyu cevapla, ardından eksikse bir sonraki adımı sor.
4. ASLA UYDURMA: Business Context'teki bilgiye sadık kal. Fiyat, süre, hizmet uydurma.
5. ASLA PASİF OLMA: "Takvimi göremiyorum", "kontrol edip döneceğim", "ekibe aktarıyorum" deme.
6. İPTAL/DEĞİŞİKLİK: Kullanıcı "iptal" veya "değiştir" derse intent'i "human_handoff" yap.
7. İSİM DÜZELTME: Sadece kullanıcı açıkça "Adımı Y olarak değiştir" derse "name" alanını güncelle. "Ben Mehmet bey" gibi selamlamalar kayıt değiştirmez.
8. RANDEVU ONAYI: "reply" alanı asla "randevunuzu oluşturdum/güncelledim" gibi kesin sistem ifadeleri içermez.
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

def call_llm_structured(system_prompt: str, user_text: str) -> dict:
    """LLM'e sadeleştirilmiş structured JSON çağrısı."""
    import requests, os, time as t
    from app.main import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL
    
    model = os.getenv("LLM_FALLBACK_MODEL") or LLM_MODEL
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    
    for model_name in [LLM_MODEL, model]:
        try:
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text}
                ],
                "temperature": 0.0,
                "max_tokens": 500
            }
            resp = requests.post(f"{LLM_BASE_URL}/chat/completions", headers=headers, json=payload, timeout=25)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            match = extract_balanced_json(content)
            if match:
                match["_llm_model_used"] = model_name
                return match
            return {
                "intent": "answer_question",
                "reply_text": content,
                "extracted_entities": {},
                "_llm_model_used": model_name,
            }
        except Exception as e:
            if model_name == model:
                raise
            continue
    return {"intent": "fallback", "reply_text": "Mesajınızı aldım.", "extracted_entities": {}}


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
        cfg = get_config()
        
        # 1. LLM'e yapısal istek gönder - Eskiden invoke_generic_llm'in yaptığı iş
        recent_history = get_recent_message_history(conn, payload.sender_id)
        recent = "\n".join([f"{msg.get('direction', 'IN').upper()}: {msg.get('message_text', '')}" 
                           for msg in (recent_history or [])[-10:]])
        
        known_context = {
            key: memory.get(key)
            for key in ["customer_goal", "requested_service", "selected_service", 
                       "service_interest", "customer_sector", "customer_subsector",
                       "contact_channel"]
            if memory.get(key)
        }
        
        available_slots = conversation.get("available_slots") or []
        slot_context = ""
        if available_slots:
            slot_lines = []
            for slot in available_slots[:12]:
                if isinstance(slot, dict):
                    slot_lines.append(f"- {slot.get('date')} {slot.get('time')}")
            slot_context = "\nMÜSAİT SLOTLAR:\n" + "\n".join(slot_lines)
        
        missing = []
        if not conversation.get("service") and not memory.get("requested_service"): 
            missing.append("service")
        if not conversation.get("lead_name") and not conversation.get("full_name"): 
            missing.append("name")
        if not conversation.get("phone"): 
            missing.append("phone")
        if not conversation.get("requested_date") or not conversation.get("requested_time"): 
            missing.append("datetime")
        
        today = datetime.datetime.now(TZ).date().strftime('%Y-%m-%d')
        business_context = json.dumps(cfg, ensure_ascii=False, default=str)
        
        system_prompt = LLM_SYSTEM_PROMPT + f"""

BUGÜN: {today}
İŞLETME: {cfg.get('business_name', 'DOEL Digital')}
BİLİNEN BAĞLAM: {json.dumps(known_context, ensure_ascii=False) if known_context else '{}'}
EKSİK BİLGİLER: {', '.join(missing) if missing else 'YOK'}
İŞLETME BİLGİSİ:
{business_context}
{slot_context}
"""
        
        # LLM çağrısı
        try:
            result_dict = call_llm_structured(system_prompt, f"KULLANICI: {message_text}\nKONUŞMA:\n{recent}")
            llm_intent = (result_dict.get("intent") or "answer_question").strip()
            llm_reply = (result_dict.get("reply_text") or result_dict.get("reply") or "").strip()
            llm_extracted = result_dict.get("extracted_entities") or result_dict.get("extracted") or {}
            metrics["llm_model_used"] = result_dict.get("_llm_model_used")
            metrics["llm_intent"] = llm_intent
        except Exception:
            llm_intent = "answer_question"
            llm_reply = ""
            llm_extracted = {}
        
        # 2. Deterministik Varlık Çıkarımı (LLM hata yaparsa diye güvenlik ağı)
        extracted_name = extract_name(message_text, conversation.get("state", "new"))
        extracted_phone = extract_phone(message_text)
        extracted_date = extract_date(message_text)
        extracted_time = extract_time(message_text)
        
        # LLM'den gelen verileri deterministik olanlarla birleştir
        effective_name = llm_extracted.get("name") or extracted_name
        effective_phone = llm_extracted.get("phone") or extracted_phone
        effective_date = llm_extracted.get("date") or extracted_date
        effective_time = llm_extracted.get("time") or extracted_time
        effective_service = llm_extracted.get("service") or conversation.get("service") or memory.get("requested_service")
        
        # 3. Basitleştirilmiş Durum Makinesi (FSM)
        has_name = bool(conversation.get("full_name") or conversation.get("lead_name") or effective_name)
        has_phone = bool(conversation.get("phone") or effective_phone)
        has_date = bool(conversation.get("requested_date") or effective_date)
        has_time = bool(conversation.get("requested_time") or effective_time)
        has_service = bool(effective_service)
        
        # Servisi otomatik tespit et ve kaydet
        if effective_service and not conversation.get("service"):
            conversation["service"] = effective_service
            memory["requested_service"] = effective_service
            memory["selected_service"] = effective_service
            memory["service_interest"] = effective_service
        
        # LLM reply'ı varsa onu kullan, yoksa FSM template'i
        reply_text = llm_reply if llm_reply else llm_intent
        
        # Kullanıcı net bir şekilde tüm bilgileri verdiyse ve randevu istiyorsa
        if has_name and has_phone and has_date and has_time and has_service:
            try:
                # Slot doğrulama
                slot_error = validate_slot(conversation.get("requested_date") or effective_date, 
                                          conversation.get("requested_time") or effective_time)
                if slot_error:
                    reply_text = slot_error
                    conversation["state"] = "collect_datetime"
                    decision_path.append("validation:slot_error")
                else:
                    # Çakışma kontrolü
                    conflict = find_existing_appointment(conn, 
                        normalize_date_string(conversation.get("requested_date") or effective_date), 
                        normalize_time_string(conversation.get("requested_time") or effective_time), 
                        conversation.get("service"))
                    if conflict:
                        alternatives = suggest_alternatives(conn, 
                            normalize_date_string(conversation.get("requested_date") or effective_date), 
                            normalize_time_string(conversation.get("requested_time") or effective_time), 
                            conversation.get("service"))
                        alt_text = ", ".join(alternatives[:3])
                        reply_text = f"Maalesef o saat dolu. Uygun seçenekler: {alt_text}. Hangisi uygun olur?"
                        conversation["state"] = "collect_datetime"
                        decision_path.append("validation:slot_conflict")
                    else:
                        # Randevu oluştur
                        if effective_date and not conversation.get("requested_date"):
                            conversation["requested_date"] = effective_date
                        if effective_time and not conversation.get("requested_time"):
                            conversation["requested_time"] = effective_time
                        conversation["state"] = "completed"
                        conversation["appointment_status"] = "confirmed"
                        created = create_appointment(conn, conversation, payload.instagram_username)
                        appointment_id = int(created[0] if isinstance(created, tuple) else created)
                        conversation["appointment_id"] = appointment_id
                        reply_text = build_confirmation_message(conversation)
                        decision_path.append("action:appointment_created")
                        queue_crm_sync(background_tasks, conversation, appointment_id, metrics)
            except HTTPException:
                raise
            except Exception as e:
                logger.exception("structured_core appointment create failed: %s", e)
                reply_text = "Randevu kaydınızı oluştururken bir sorun oluştu, ekibimize iletiyorum. En kısa sürede dönüş yapacağız."
                conversation["state"] = "human_handoff"
                decision_path.append("error:appointment_create_failed")
        elif not llm_reply:
            # LLM cevap vermediyse FSM template'i ile devam et
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

        # Bellek ve veritabanı güncellemeleri
        if effective_name and not conversation.get("full_name"):
            conversation["lead_name"] = effective_name
            conversation["full_name"] = effective_name
        if effective_phone and not conversation.get("phone"):
            conversation["phone"] = effective_phone
        if effective_date and not conversation.get("requested_date"):
            conversation["requested_date"] = effective_date
        if effective_time and not conversation.get("requested_time"):
            conversation["requested_time"] = effective_time
            
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