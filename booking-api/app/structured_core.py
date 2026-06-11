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
    collect_next_booking_slot_options, remember_booking_slot_options, normalize_booking_slot_option,
    normalize_booking_kind, infer_booking_kind, DIRECT_APPOINTMENT_KEYWORDS
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

    try:
        with get_conn() as conn:
            conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
            sanitize_conversation_state(conversation)
            memory = ensure_conversation_memory(conversation)
            conversation["instagram_user_id"] = payload.sender_id
            cfg = get_config()
            
            # 1. LLM'e yapısal istek gönder
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
            
            # 2. Deterministik Varlık Çıkarımı
            extracted_name = extract_name(message_text, conversation.get("state", "new"))
            extracted_phone = extract_phone(message_text)
            # Fallback regex: "05XXXXXXXXX" formatında telefon
            if not extracted_phone:
                _phone_raw = re.search(r'\b0?\d{10,11}\b', sanitize_text(message_text))
                if _phone_raw:
                    extracted_phone = _phone_raw.group(0)
            # Fallback: "İsim Soyisim, 05XXXXXXXXX" formatında aynı mesajda isim+telefon
            if not extracted_phone:
                _combo = re.search(r'([A-ZÇĞİÖŞÜ][a-zçğıöşü]+\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+)\s*[,;]\s*(\d{10,11})', message_text)
                if _combo:
                    extracted_phone = _combo.group(2)
            extracted_date = extract_date(message_text)
            extracted_time = extract_time(message_text)
            # Ham noktalı saat formatı fallback (extract_time bazen kaçırabiliyor)
            if not extracted_time:
                _dot_time = re.search(r'\b(\d{1,2})\.(\d{2})\b', message_text)
                if _dot_time:
                    extracted_time = f"{int(_dot_time.group(1)):02d}:{_dot_time.group(2)}"
            
            effective_name = llm_extracted.get("name") or extracted_name
            # İsim temizleme: "Zeynep Kaya olacak", "adım X olsun" gibi ifadeleri temizle
            if effective_name:
                effective_name = re.sub(r'\s+(olacak|olur|olsun|yap|yapal[ıi]m|yapalim|ekler|ekle|misin|musun|m[ıi]s[ıi]n|musun)\b.*$', '', effective_name, flags=re.IGNORECASE).strip()
            if not effective_name:
                _name_from_msg = extract_name(message_text, conversation.get("state", "new"))
                if _name_from_msg:
                    _name_from_msg = re.sub(r'\s+(olacak|olur|olsun|yap|yapal[ıi]m|yapalim|ekler|ekle|misin|musun|m[ıi]s[ıi]n|musun)\b.*$', '', _name_from_msg, flags=re.IGNORECASE).strip()
                    if len(_name_from_msg.split()) >= 2:
                        effective_name = _name_from_msg
            effective_phone = llm_extracted.get("phone") or extracted_phone
            effective_date = llm_extracted.get("date") or extracted_date
            effective_time = llm_extracted.get("time") or extracted_time
            # Normalize dotted time format (17.00 → 17:00)
            if effective_time and "." in effective_time:
                effective_time = effective_time.replace(".", ":")
            effective_service = llm_extracted.get("service") or conversation.get("service") or memory.get("requested_service")
            # Fallback: kullanıcı mesajından servis tespiti (LLM bulamadıysa)
            if not effective_service:
                from app.generic_core import detect_requested_service_from_text
                effective_service = detect_requested_service_from_text(message_text, cfg)
            
            deterministic_handled = False

            # Deterministik geçmiş saat kontrolü
            if effective_date and effective_time:
                try:
                    _now = datetime.datetime.now(TZ)
                    _req_date = datetime.date.fromisoformat(normalize_date_string(effective_date))
                    _req_time = datetime.time.fromisoformat(normalize_time_string(effective_time))
                    if _req_date == _now.date() and _req_time <= _now.time():
                        reply_text = f"Bugün için geçmiş bir saat seçilemez. Şu an saat {_now.strftime('%H:%M')}. Yarın veya ileri bir tarih için yazın."
                        conversation["state"] = "collect_datetime"
                        effective_date = None
                        effective_time = None
                        deterministic_handled = True
                        decision_path.append("validation:past_time_rejected")
                except Exception:
                    pass
            
            # 3. Basitleştirilmiş FSM
            has_name = bool(conversation.get("full_name") or conversation.get("lead_name") or effective_name)
            has_phone = bool(conversation.get("phone") or effective_phone)
            has_date = bool(conversation.get("requested_date") or effective_date)
            has_time = bool(conversation.get("requested_time") or effective_time)
            has_service = bool(effective_service)
            
            # effective_date/time varsa hemen conversation'a yaz (sonraki turlar için)
            if effective_date and not conversation.get("requested_date"):
                conversation["requested_date"] = effective_date
            if effective_time and not conversation.get("requested_time"):
                conversation["requested_time"] = effective_time
            # Slot conflict durumunda kullanıcı yeni saat verdiyse eskiyi ezip yenisini yaz
            # AMA sadece tarih DEĞİŞMEDİYSE - search_dates bugünü false döndürebilir
            if effective_time and conversation.get("requested_time") and effective_time != conversation.get("requested_time"):
                conversation["requested_time"] = effective_time
                conversation.pop("available_slots", None)
            if effective_date and effective_date != normalize_date_string(datetime.datetime.now(TZ).date().isoformat()):
                conversation["requested_date"] = effective_date
                conversation.pop("available_slots", None)
            
            if effective_service and not conversation.get("service"):
                conversation["service"] = effective_service
                memory["requested_service"] = effective_service
                memory["selected_service"] = effective_service
                memory["service_interest"] = effective_service
            
            reply_text = llm_reply if llm_reply else ""
            
            # İsim düzeltme tespiti - FSM'e girmeden önce kontrol et
            _is_name_correction = False
            if effective_name and conversation.get("full_name") and effective_name != conversation.get("full_name"):
                _name_lowered = sanitize_text(message_text).lower()
                if any(w in _name_lowered for w in ("adım", "adim", "ismim", "ismini", "düzeltt", "duzeltt", "değişti", "degisti", "değil", "degil", "olarak", "kaydet")):
                    _is_name_correction = True
            
            # Deterministik direkt yanıtlar (LLM bypass) - bunlardan biri tetiklendiyse FSM'ye girme
            from app.generic_core import is_who_to_call_question, build_who_to_call_reply, is_existing_appointment_recall_question, build_existing_appointment_recall_reply
            if is_who_to_call_question(message_text):
                reply_text = build_who_to_call_reply(conn)
                decision_path.append("direct:who_to_call")
                deterministic_handled = True
            elif is_existing_appointment_recall_question(message_text):
                recall = build_existing_appointment_recall_reply(conn, conversation)
                if recall:
                    reply_text = recall
                    decision_path.append("direct:appointment_recall")
                    deterministic_handled = True
            
            # Handoff kontrolü - LLM'den bağımsız deterministik tespit
            from app.handoff import is_handoff_request, build_handoff_reply
            if not deterministic_handled and is_handoff_request(message_text):
                reply_text = "Size yardımcı olması için sizi DOEL Digital ekibinden bir yetkiliye yönlendiriyorum. En kısa sürede size dönüş yapacaklar."
                conversation["state"] = "human_handoff"
                conversation["assigned_human"] = True
                decision_path.append("direct:handoff")
                deterministic_handled = True
            
            # İsim/telefon/tarih/saat alanlarını her zaman güncelle (state'ten bağımsız)
            if effective_name:
                if not conversation.get("full_name"):
                    conversation["full_name"] = effective_name
                    conversation["lead_name"] = effective_name
                elif effective_name != conversation.get("full_name"):
                    conversation["full_name"] = effective_name
                    conversation["lead_name"] = effective_name
                    # İsim değiştiyse appointment tablosunu da güncelle
                    if conversation.get("appointment_id"):
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE appointments SET full_name = %s, updated_at = NOW() WHERE id = %s",
                                    (effective_name, conversation["appointment_id"]),
                                )
                        except Exception:
                            pass
            if effective_phone and not conversation.get("phone"):
                conversation["phone"] = effective_phone
            if effective_date and not conversation.get("requested_date") and conversation.get("state") != "completed":
                conversation["requested_date"] = effective_date
            if effective_time and not conversation.get("requested_time") and conversation.get("state") != "completed":
                conversation["requested_time"] = effective_time
            
            # İsim düzeltme tespiti - FSM'e girmeden önce kontrol et
            _is_name_correction = False
            _name_correction_regex = re.search(
                r'(?:ad[ıi]m[ıi]?\s+)?(?:asl[ıi]nda\s+)?[A-ZÇĞİÖŞÜ][a-zçğıöşü]+\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+',
                message_text
            )
            if effective_name and effective_name != conversation.get("full_name"):
                _name_lowered = sanitize_text(message_text).lower()
                if any(w in _name_lowered for w in ("adım", "adim", "ismim", "ismini", "düzeltt", "duzeltt", "değişti", "degisti", "değil", "degil")):
                    _is_name_correction = True
            
            # Completed state'te yeni booking opt-in varsa, eski alanları tamamen temizle
            _new_booking_reset = (
                conversation.get("state") == "completed" 
                and any(w in sanitize_text(message_text).lower() for w in ("randevu", "görüşme", "gorusme", "alalım", "alayım", "planla", "ayarla", "yeni"))
                and not deterministic_handled
            )
            if _new_booking_reset:
                for _key in ("requested_date", "requested_time", "service", "appointment_id", "appointment_status"):
                    conversation.pop(_key, None)
                conversation["state"] = "collect_service"
                conversation["appointment_status"] = "collecting"
                memory.pop("requested_service", None)
                memory.pop("selected_service", None)
                memory.pop("service_interest", None)
                decision_path.append("fsm:completed_new_booking_reset")
                effective_date = None
                effective_time = None
                has_date = False
                has_time = False
                has_service = False
            
            if not deterministic_handled and has_name and has_phone and has_date and has_time and has_service and conversation.get("state") != "completed" and not _is_name_correction:
                if not conversation.get("requested_date") or not conversation.get("requested_time"):
                    pass
                else:
                    from datetime import date as date_cls, time as time_cls, timedelta
                    slot_error = validate_slot(conversation.get("requested_date"), conversation.get("requested_time"))
                    if not slot_error:
                        try:
                            req_date = date_cls.fromisoformat(normalize_date_string(conversation.get("requested_date")))
                            req_time = time_cls.fromisoformat(normalize_time_string(conversation.get("requested_time")))
                            now = datetime.datetime.now(TZ)
                            # Sadece BUGÜN için geçmiş saat kontrolü - yarınki randevularda bu kontrol atlanır
                            if req_date == now.date() and req_time <= now.time():
                                slot_error = f"Bugün için geçmiş bir saat seçilemez. Şu an saat {now.strftime('%H:%M')}. Yarın veya ileri bir tarih için yazın."
                        except Exception:
                            pass
                    if slot_error:
                        reply_text = slot_error
                        conversation["state"] = "collect_datetime"
                        decision_path.append("validation:slot_error")
                    else:
                        try:
                            conversation["state"] = "completed"
                            conversation["appointment_status"] = "confirmed"
                            existing_kind = normalize_booking_kind(conversation.get("booking_kind"))
                            if existing_kind == "preconsultation":
                                if any(kw in sanitize_text(message_text).lower() for kw in DIRECT_APPOINTMENT_KEYWORDS):
                                    conversation["booking_kind"] = "appointment"
                                    decision_path.append("booking_kind:overridden_to_appointment")
                                else:
                                    conversation["booking_kind"] = "preconsultation"
                            elif existing_kind == "appointment":
                                conversation["booking_kind"] = "appointment"
                            else:
                                inferred = infer_booking_kind(message_text, {}, conversation)
                                conversation["booking_kind"] = inferred or "appointment"
                                decision_path.append(f"booking_kind:inferred:{conversation['booking_kind']}")
                            created = create_appointment(conn, conversation, payload.instagram_username)
                            appointment_id = int(created[0] if isinstance(created, tuple) else created)
                            conversation["appointment_id"] = appointment_id
                            reply_text = build_confirmation_message(conversation)
                            decision_path.append("action:appointment_created")
                            queue_crm_sync(background_tasks, conversation, appointment_id, metrics)
                            # Live CRM'e de yaz (Vercel panelin okuduğu Supabase)
                            try:
                                from app.main import live_crm_upsert_preconsultation
                                live_crm_upsert_preconsultation(conversation)
                                decision_path.append("action:live_crm_synced")
                            except Exception:
                                logger.exception("live_crm_sync_failed_in_structured_core")
                        except HTTPException as http_exc:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            conversation["state"] = "collect_datetime"
                            conversation["appointment_status"] = "collecting"
                            req_t = normalize_time_string(conversation.get("requested_time")) or "o saat"
                            req_d = normalize_date_string(conversation.get("requested_date"))
                            alt_slots = []
                            if req_d and req_t:
                                try:
                                    alt_slots = suggest_alternatives(conn, req_d, req_t, conversation.get("service"))
                                    alt_slots = [s for s in alt_slots if s != req_t][:4]
                                except Exception:
                                    pass
                            if alt_slots:
                                alt_text = ", ".join(alt_slots)
                                reply_text = f"{req_t} dolu. Aynı gün boş: {alt_text}. Hangisi uygun olur?"
                            else:
                                reply_text = f"{req_t} dolu ve o gün boş slot kalmadı. Başka bir gün yazar mısınız?"
                            conversation.pop("requested_time", None)
                            decision_path.append("validation:slot_conflict_create")
                            decision_path.append("validation:slot_conflict_create")
            
            # LLM boş döndüyse ve FSM de reply vermediyse - akıllı fallback
            if not reply_text or not reply_text.strip():
                _state = conversation.get("state", "new")
                _lowered = sanitize_text(message_text).lower()
                if _state == "completed":
                    reply_text = "Başka bir konuda yardımcı olabilir miyim?"
                elif any(w in _lowered for w in ("soyad", "soyadım", "soyadim", "adım", "adim", "ismim", "düzelt", "duzelt", "değiş", "degis")):
                    reply_text = "İsim bilginizi güncelledim. Başka bir konuda yardımcı olabilir miyim?"
                elif any(w in _lowered for w in ("randevum", "kayıtlı", "kayitli", "ne zaman", "saat kaç", "hangi isim")):
                    reply_text = "Randevu bilgilerinizi kontrol edip size dönüş yapacağım."
                elif _state == "collect_datetime":
                    reply_text = "Uygun gün ve saati yazar mısınız?"
                else:
                    reply_text = "Mesajınızı aldım, nasıl yardımcı olabilirim?"

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
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("structured_core top-level failure: %s", e)
        metrics["total_ms"] = elapsed_ms(request_started_at)
        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=True,
            reply_text="Mesajınızı aldım, en kısa sürede dönüş yapacağım.",
            outbound_text="Mesajınızı aldım, en kısa sürede dönüş yapacağım.",
            llm_raw_reply_text="",
            final_reply_source="structured_core",
            handoff=True,
            conversation_state="human_handoff",
            appointment_created=False,
            appointment_id=None,
            normalized={},
            metrics=metrics,
            decision_path=decision_path + ["error:top_level_fallback"],
        )