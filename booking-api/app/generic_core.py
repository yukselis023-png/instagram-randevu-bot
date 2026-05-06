import os
import json
import re
import datetime
import requests
import os
import logging
import time as time_module
from typing import Any, Tuple, Optional

from fastapi import BackgroundTasks
from app.main import (
    ProcessResult, IncomingMessage, get_conn, get_or_create_conversation, 
    sanitize_conversation_state, ensure_conversation_memory, 
    sync_conversation_memory_summary, has_processed_inbound_message,
    try_acquire_inbound_processing_lock, save_message_log, get_recent_message_history,
    should_reset_stale_conversation, reset_conversation_for_restart, build_normalized,
    update_conversation_memory_after_bot_reply, upsert_conversation, upsert_customer_from_conversation,
    schedule_customer_automation_events, sanitize_text, extract_inbound_message_id, extract_inbound_platform,
    build_inbound_dedupe_key, elapsed_ms, queue_crm_sync, get_config, call_llm_content,
    is_company_capability_question, build_company_capability_reply, is_simple_greeting,
    is_business_fit_question, recommendation_engine, extract_name, extract_phone,
    is_invalid_phone_attempt, extract_date, extract_time_for_state, extract_time, create_appointment,
    build_confirmation_message, try_reschedule_confirmed_appointment, find_active_appointment_for_user
)

logger = logging.getLogger(__name__)

BOOKING_OPT_IN_PHRASES = (
    "olur görüşelim",
    "olur goruselim",
    "görüşelim",
    "goruselim",
    "randevu alalım",
    "randevu alalim",
    "ön görüşme",
    "on gorusme",
)

SERVICE_REPEAT_QUESTION_FRAGMENTS = (
    "hangi hizmet",
    "hizmeti araştırıyorsunuz",
    "hizmeti arastiriyorsunuz",
    "hangi konuda bilgi",
)


def is_booking_opt_in(message_text: str, intent: str | None) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    return intent == "booking_request" or any(phrase in lowered for phrase in BOOKING_OPT_IN_PHRASES)


def known_requested_service(conversation: dict[str, Any], memory: dict[str, Any]) -> str | None:
    for key in ("requested_service", "selected_service", "service_interest"):
        value = sanitize_text(memory.get(key) or "")
        if value:
            return value
    value = sanitize_text(conversation.get("service") or "")
    return value or None


def detect_requested_service_from_text(message_text: str, cfg: dict[str, Any]) -> str | None:
    lowered = sanitize_text(message_text or "").lower()
    if not lowered:
        return None
    for service in cfg.get("service_catalog", []) or []:
        if not isinstance(service, dict):
            continue
        display = sanitize_text(service.get("display") or service.get("name") or "")
        candidates = [display, service.get("name"), service.get("slug")]
        candidates.extend(service.get("keywords") or [])
        for candidate in candidates:
            clean = sanitize_text(str(candidate or "")).lower()
            if clean and clean in lowered:
                if clean == "otomasyon" or "otomasyon" in clean:
                    return "Otomasyon"
                return display or sanitize_text(str(candidate or ""))
    return None


def remember_requested_service(conversation: dict[str, Any], memory: dict[str, Any], service_label: str | None) -> str | None:
    clean = sanitize_text(service_label or "")
    if not clean:
        return None
    memory["requested_service"] = clean
    memory["selected_service"] = clean
    memory["service_interest"] = clean
    if not sanitize_text(conversation.get("service") or ""):
        conversation["service"] = clean
    return clean


def service_reply_phrase(service_label: str | None) -> str:
    lowered = sanitize_text(service_label or "").lower()
    if "otomasyon" in lowered:
        return "otomasyon"
    return lowered or "bu hizmet"


def reply_repeats_service_question(reply_text: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    return any(fragment in lowered for fragment in SERVICE_REPEAT_QUESTION_FRAGMENTS)


def reply_question_count(reply_text: str | None) -> int:
    return sanitize_text(reply_text or "").count("?")


def reply_sentence_count(reply_text: str | None) -> int:
    text = sanitize_text(reply_text or "").strip()
    if not text:
        return 0
    endings = {".", "?", "!"}
    count = sum(1 for char in text if char in endings)
    return count or 1


def build_generic_business_context(message_text: str, cfg: dict[str, Any]) -> str:
    context = dict(cfg or {})
    if not is_company_capability_question(message_text):
        context.pop("unavailable_services", None)
    return json.dumps(context, ensure_ascii=False)


def strip_leading_greeting_for_non_greeting(message_text: str, reply_text: str | None) -> str:
    reply = reply_text or ""
    if not sanitize_text(reply) or is_simple_greeting(message_text):
        return reply
    lowered = sanitize_text(reply).lower()
    for prefix in ("merhaba,", "merhaba.", "merhaba ", "selam,", "selam.", "selam "):
        if lowered.startswith(prefix):
            stripped = reply[len(prefix):].strip()
            if stripped:
                return stripped[:1].upper() + stripped[1:]
    return reply


def build_service_carryover_booking_reply(service_label: str | None, state: str | None) -> str:
    service = service_reply_phrase(service_label)
    if state == "collect_phone":
        return f"Harika, {service} için ön görüşme oluşturalım. Telefon numaranızı alabilir miyim?"
    return f"Harika, {service} için ön görüşme oluşturalım. Ad soyadınızı alabilir miyim?"


def is_llm_error_reply(reply_text: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    return lowered.startswith("error:") or "llm json error" in lowered or "too many requests" in lowered


def extract_generic_datetime_time(message_text: str) -> str | None:
    lowered = sanitize_text(message_text or "").lower()
    match = re.search(r"\b(akşam|aksam|sabah|öğlen|oglen)?\s*(\d{1,2})(?::(\d{2}))?\b", lowered)
    if not match:
        return None
    period, hour_text, minute_text = match.groups()
    hour = int(hour_text)
    minute = int(minute_text or "0")
    if period in {"akşam", "aksam"} and 1 <= hour <= 11:
        hour += 12
    if period in {"öğlen", "oglen"} and hour == 12:
        hour = 12
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return f"{hour:02d}:{minute:02d}"
    return None


def is_confirmed_generic_appointment(conversation: dict[str, Any]) -> bool:
    return bool(
        conversation.get("appointment_id")
        or conversation.get("appointment_status") == "confirmed"
        or conversation.get("state") == "completed"
    )


def existing_generic_appointment_id(conversation: dict[str, Any], conn: Any | None = None) -> int | None:
    try:
        if conversation.get("appointment_id"):
            return int(conversation.get("appointment_id"))
    except Exception:
        pass
    if conn is not None and conversation.get("instagram_user_id"):
        try:
            appointment = find_active_appointment_for_user(
                conn,
                conversation.get("instagram_user_id"),
                preferred_date=conversation.get("requested_date"),
                preferred_time=conversation.get("requested_time"),
            )
            if appointment and appointment.get("id"):
                return int(appointment["id"])
        except Exception:
            return None
    return None


def is_explicit_reschedule_request(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    subject = any(token in lowered for token in ["randevu", "gorusme", "görüşme", "on gorusme", "ön görüşme", "saat"])
    action = any(token in lowered for token in ["degistir", "değiştir", "guncelle", "güncelle", "yap", "kaydir", "kaydır", "cek", "çek"])
    return subject and action


def is_reschedule_confirmation_acceptance(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip()
    return lowered in {"evet", "onayliyorum", "onaylıyorum", "tamam", "olur", "aynen"}


def detect_reschedule_candidate(message_text: str, extracted: dict[str, Any]) -> tuple[str | None, str | None]:
    detected_date = extract_date(message_text) or extracted.get("requested_date")
    detected_time = (
        extract_time_for_state(message_text, "collect_datetime")
        or extract_time(message_text)
        or extract_generic_datetime_time(message_text)
        or extracted.get("requested_time")
    )
    return detected_date, detected_time


def append_reschedule_handoff_note(conversation: dict[str, Any], requested_date: str | None, requested_time: str | None) -> None:
    note = "müşteri saat değişikliği istedi"
    if requested_date or requested_time:
        note = f"{note}: {requested_date or conversation.get('requested_date') or '-'} {requested_time or conversation.get('requested_time') or '-'}"
    current = sanitize_text(conversation.get("notes") or "")
    conversation["notes"] = f"{current}\n{note}".strip() if current else note
    conversation["assigned_human"] = True
    memory = ensure_conversation_memory(conversation)
    memory["pending_reschedule_request"] = {"requested_date": requested_date, "requested_time": requested_time, "note": note}
    memory["open_loop"] = "handoff"
    conversation["memory_state"] = memory


def build_reschedule_confirmation_question(conversation: dict[str, Any], requested_date: str | None, requested_time: str | None) -> str:
    target_date = requested_date or conversation.get("requested_date")
    target_time = requested_time or conversation.get("requested_time")
    if requested_date and requested_time:
        return f"Mevcut ön görüşmenizi {target_date} saat {target_time} olarak değiştirmek istediğinizi onaylıyor musunuz?"
    if requested_time:
        return f"Mevcut ön görüşme saatinizi {target_time} olarak değiştirmek istediğinizi onaylıyor musunuz?"
    return "Mevcut ön görüşme tarihinizi değiştirmek istediğinizi onaylıyor musunuz?"


def handle_confirmed_generic_reschedule(
    conn: Any,
    conversation: dict[str, Any],
    memory: dict[str, Any],
    message_text: str,
    extracted: dict[str, Any],
    username: str | None,
) -> dict[str, Any] | None:
    if not is_confirmed_generic_appointment(conversation):
        return None

    existing_id = existing_generic_appointment_id(conversation, conn)
    pending_confirm = memory.get("open_loop") == "generic_reschedule_confirmation_pending"
    if pending_confirm and is_reschedule_confirmation_acceptance(message_text):
        requested_date = memory.get("reschedule_requested_date") or conversation.get("requested_date")
        requested_time = memory.get("reschedule_requested_time") or conversation.get("requested_time")
        update_text = f"randevuyu {requested_date or ''} {requested_time or ''} yap".strip()
        try:
            updated, reply, label = try_reschedule_confirmed_appointment(conn, conversation, update_text, username)
        except Exception:
            updated, reply, label = False, "", None
        if updated:
            return {"handled": True, "reply_text": reply, "handoff": False, "appointment_id": existing_id, "decision_label": label or "appointment_rescheduled"}
        append_reschedule_handoff_note(conversation, requested_date, requested_time)
        return {
            "handled": True,
            "reply_text": "Saat değişikliği talebinizi ekibe iletmek üzere not aldım. Mevcut randevu kaydınız korunuyor.",
            "handoff": True,
            "appointment_id": existing_id,
            "decision_label": "appointment_reschedule_handoff",
        }

    requested_date, requested_time = detect_reschedule_candidate(message_text, extracted)
    if not requested_date and not requested_time:
        return None

    if is_explicit_reschedule_request(message_text):
        try:
            updated, reply, label = try_reschedule_confirmed_appointment(conn, conversation, message_text, username)
        except Exception:
            updated, reply, label = False, "", None
        if updated:
            return {"handled": True, "reply_text": reply, "handoff": False, "appointment_id": existing_id, "decision_label": label or "appointment_rescheduled"}
        append_reschedule_handoff_note(conversation, requested_date, requested_time)
        return {
            "handled": True,
            "reply_text": "Mevcut randevu kaydınızı koruyorum; saat değişikliği talebinizi ekibe iletmek üzere not aldım.",
            "handoff": True,
            "appointment_id": existing_id,
            "decision_label": "appointment_reschedule_handoff",
        }

    memory["reschedule_requested_date"] = requested_date or conversation.get("requested_date")
    memory["reschedule_requested_time"] = requested_time or conversation.get("requested_time")
    memory["open_loop"] = "generic_reschedule_confirmation_pending"
    conversation["memory_state"] = memory
    return {
        "handled": True,
        "reply_text": build_reschedule_confirmation_question(conversation, requested_date, requested_time),
        "handoff": False,
        "appointment_id": existing_id,
        "decision_label": "appointment_reschedule_confirm_required",
    }


def build_active_booking_prompt_reply(conversation: dict[str, Any], memory: dict[str, Any]) -> str | None:
    state = conversation.get("state")
    service = service_reply_phrase(known_requested_service(conversation, memory))
    if state == "collect_service":
        return "Ön görüşme için hangi hizmeti düşünüyorsunuz: web tasarım, otomasyon, reklam veya sosyal medya mı?"
    if state == "collect_name":
        return f"Harika, {service} için ön görüşme oluşturalım. Ad soyadınızı alabilir miyim?"
    if state == "collect_phone":
        return f"Harika, {service} için ön görüşme oluşturalım. Telefon numaranızı eksiksiz alabilir miyim?"
    if state == "collect_datetime":
        return "Uygun gün ve saati yazar mısınız? Örneğin yarın 13:00 gibi."
    return None


def process_instagram_message_generic(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    request_started_at = time_module.perf_counter()
    metrics = {
        "reply_engine": "generic_core",
        "total_ms": 0,
        "message_type": "reply"
    }

    trace_id = sanitize_text(payload.trace_id or ((payload.raw_event or {}).get("trace_id") if isinstance(payload.raw_event, dict) else "") or payload.sender_id)
    message_text = sanitize_text(payload.message_text or "")
    if not message_text:
        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=False,
            reply_text=None,
            handoff=False,
            conversation_state="ignored",
            normalized={},
            metrics=metrics,
            decision_path=["ignored:empty"]
        )

    inbound_message_id = extract_inbound_message_id(payload.raw_event)
    inbound_platform = extract_inbound_platform(payload.raw_event)
    
    with get_conn() as conn:
        conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
        sanitize_conversation_state(conversation)
        memory = ensure_conversation_memory(conversation)
        sync_conversation_memory_summary(conversation)

        processing_lock_acquired = try_acquire_inbound_processing_lock(conn, inbound_platform, payload.sender_id, inbound_message_id)
        if not processing_lock_acquired:
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=False,
                reply_text=None,
                handoff=False,
                conversation_state=conversation.get("state", "new"),
                normalized=build_normalized(conversation),
                metrics=metrics,
                decision_path=["duplicate_inflight_ignored"]
            )
            
        duplicate_inbound = has_processed_inbound_message(conn, inbound_platform, payload.sender_id, inbound_message_id)
        if duplicate_inbound:
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=False,
                reply_text=None,
                handoff=False,
                conversation_state=conversation.get("state", "new"),
                normalized=build_normalized(conversation),
                metrics=metrics,
                decision_path=["duplicate_ignored"]
            )

        if should_reset_stale_conversation(conversation, message_text):
            reset_conversation_for_restart(conversation, clear_identity=True)
            memory = ensure_conversation_memory(conversation)
            
        save_message_log(conn, payload.sender_id, "in", message_text, payload.raw_event or {})
        recent_history = get_recent_message_history(conn, payload.sender_id)

        # 1. CORE LLM & INTENT RECOGNITION
        result_dict = invoke_generic_llm(message_text, conversation, memory, recent_history)
        
        intent = result_dict.get("intent", "fallback")
        reply_text = result_dict.get("reply_text", "Anlaşıldı.")
        extracted = result_dict.get("extracted_entities", {})

        decision_path = [f"generic_intent:{intent}"]
        booking_opt_in = is_booking_opt_in(message_text, intent)
        deterministic_reply = False
        if is_company_capability_question(message_text):
            reply_text = build_company_capability_reply(message_text)
            decision_path.append("reply:company_capability")
            deterministic_reply = True
        elif is_business_fit_question(message_text):
            reply_text = recommendation_engine(conversation, message_text, recent_history)
            decision_path.append("reply:business_fit")
            deterministic_reply = True

        post_confirmation = handle_confirmed_generic_reschedule(conn, conversation, memory, message_text, extracted, payload.instagram_username)
        if post_confirmation:
            reply_text = post_confirmation["reply_text"]
            handoff = bool(post_confirmation.get("handoff"))
            decision_path.append(str(post_confirmation.get("decision_label") or "appointment_reschedule_followup"))
            update_conversation_memory_after_bot_reply(conversation, reply_text, "|".join(decision_path))
            upsert_conversation(conn, conversation)
            crm_customer = upsert_customer_from_conversation(conn, conversation)
            if crm_customer:
                schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector", ""))
            save_message_log(conn, payload.sender_id, "out", reply_text, {"type": "reply", "decision_path": decision_path})
            metrics["total_ms"] = elapsed_ms(request_started_at)
            queue_crm_sync(background_tasks, conversation, post_confirmation.get("appointment_id"), metrics)
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=True,
                reply_text=reply_text,
                handoff=handoff,
                conversation_state=conversation.get("state", "new"),
                appointment_created=False,
                appointment_id=post_confirmation.get("appointment_id"),
                normalized=build_normalized(conversation),
                metrics=metrics,
                decision_path=decision_path,
            )

        # 2. STATE & CRM DETERMINISTIC LAYER
        handoff = False
        active_booking_state = str(conversation.get("state") or "").startswith("collect_")
        if not deterministic_reply and not active_booking_state and intent == "human_handoff":
            decision_path.append("action:handoff")
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["appointment_status"] = "handoff"
            handoff = True

        # Sync deterministic entities immediately. Booking fields must not depend only on LLM extraction.
        state_before_entities = conversation.get("state", "new")
        deterministic_name = extract_name(message_text, state_before_entities)
        llm_name = extracted.get("lead_name") if state_before_entities == "collect_name" or not conversation.get("full_name") else None
        name_candidate = deterministic_name or llm_name
        if name_candidate:
            conversation["lead_name"] = name_candidate
            conversation["full_name"] = name_candidate
            decision_path.append("detected:name" if deterministic_name else "extracted:name")
        phone_candidate = extract_phone(message_text) or extract_phone(str(extracted.get("phone") or ""))
        invalid_phone_attempt = is_invalid_phone_attempt(message_text, state_before_entities)
        if phone_candidate:
            conversation["phone"] = phone_candidate
            decision_path.append("detected:phone" if extract_phone(message_text) else "extracted:phone")
        detected_service = extracted.get("requested_service") or detect_requested_service_from_text(message_text, get_config())
        if detected_service:
            remember_requested_service(conversation, memory, detected_service)
            decision_path.append("extracted:service" if extracted.get("requested_service") else "detected:service")
        elif booking_opt_in:
            carried_service = remember_requested_service(conversation, memory, known_requested_service(conversation, memory))
            if carried_service:
                decision_path.append("carried:service")
        direct_date = extract_date(message_text)
        llm_date = extracted.get("requested_date")
        date_candidate = direct_date or llm_date
        if date_candidate and (direct_date or not conversation.get("requested_date")):
            try:
                datetime.datetime.strptime(date_candidate, "%Y-%m-%d")
                conversation["requested_date"] = date_candidate
                decision_path.append("detected:date" if direct_date else "extracted:date")
            except Exception:
                pass
        direct_time = extract_time_for_state(message_text, state_before_entities) or extract_time(message_text) or extract_generic_datetime_time(message_text)
        time_candidate = direct_time or extracted.get("requested_time")
        if time_candidate:
            try:
                datetime.datetime.strptime(time_candidate, "%H:%M")
                conversation["requested_time"] = time_candidate
                decision_path.append("detected:time" if direct_time else "extracted:time")
            except Exception:
                pass
        if extracted.get("customer_goal"):
            memory["customer_goal"] = extracted["customer_goal"]

        conversation["memory_state"] = memory

        # 3. BOOKING FINITE STATE MACHINE (Only if booking intent or inside active flow)
        appointment_created = False
        appointment_id = None
        curr_state = conversation.get("state", "new")
        state_changed_by_fsm = False
        invalid_phone_prompt = False
        
        if not handoff and (booking_opt_in or intent in ["booking_request", "active_booking"] or curr_state.startswith("collect_")):
            carried_service = remember_requested_service(conversation, memory, known_requested_service(conversation, memory))
            has_service = bool(carried_service)
            has_phone = bool(conversation.get("phone"))
            has_name = bool(conversation.get("full_name") or conversation.get("lead_name"))
            has_date = bool(conversation.get("requested_date"))
            has_time = bool(conversation.get("requested_time"))

            previous_state = conversation.get("state", "new")
            if invalid_phone_attempt and not has_phone:
                conversation["state"] = "collect_phone"
                invalid_phone_prompt = True
            elif not has_service:
                conversation["state"] = "collect_service"
            elif not has_name:
                conversation["state"] = "collect_name"
            elif not has_phone:
                conversation["state"] = "collect_phone"
            elif not has_date or not has_time:
                conversation["state"] = "collect_datetime"
            else:
                conversation["state"] = "completed"
                conversation["appointment_status"] = "confirmed"
                appointment_created = True
                appointment_id, _live_crm_ms = create_appointment(conn, conversation, payload.instagram_username)
                conversation["appointment_id"] = appointment_id
                decision_path.append("action:appointment_confirmed")
            state_changed_by_fsm = conversation.get("state") != previous_state

        service_for_booking = known_requested_service(conversation, memory)
        if appointment_created:
            reply_text = build_confirmation_message(conversation)
            decision_path.append("fsm:confirmation_reply")
        elif (
            booking_opt_in
            and service_for_booking
            and conversation.get("state") in {"collect_name", "collect_phone"}
        ):
            reply_text = build_service_carryover_booking_reply(service_for_booking, conversation.get("state"))
            decision_path.append("fsm:service_carryover_booking")
        elif (curr_state.startswith("collect_") and (is_llm_error_reply(reply_text) or state_changed_by_fsm or invalid_phone_prompt)):
            active_prompt = build_active_booking_prompt_reply(conversation, memory)
            if active_prompt:
                reply_text = active_prompt
                decision_path.append("fsm:active_booking_prompt")

        # 4. FINAL QUALITY GUARD (Post FSM Check)
        reply_text, guard_label = generic_quality_guard(reply_text, extracted, memory, get_config(), message_text)
        if guard_label:
            decision_path.append(f"guard:{guard_label}")

        update_conversation_memory_after_bot_reply(conversation, reply_text, "|".join(decision_path))

        # Output payload syncs
        upsert_conversation(conn, conversation)
        crm_customer = upsert_customer_from_conversation(conn, conversation)
        if crm_customer:
            schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector", ""))
            
        save_message_log(conn, payload.sender_id, "out", reply_text, {"type": "reply", "decision_path": decision_path})

        metrics["total_ms"] = elapsed_ms(request_started_at)
        queue_crm_sync(background_tasks, conversation, None, metrics)

        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=True,
            reply_text=reply_text,
            handoff=handoff,
            conversation_state=conversation.get("state", "new"),
            appointment_created=appointment_created,
            appointment_id=appointment_id,
            normalized=build_normalized(conversation),
            metrics=metrics,
            decision_path=decision_path,
        )


def call_llm_json(system_prompt: str, user_text: str) -> dict:
    import requests, os, json, re
    llm_url = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1")
    llm_key = os.getenv("LLM_API_KEY", "")
    llm_model = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    if not llm_key:
        from app.main import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL
        llm_url = LLM_BASE_URL
        llm_key = LLM_API_KEY
        llm_model = LLM_MODEL

    headers = {"Authorization": f"Bearer {llm_key}", "Content-Type": "application/json"}
    payload = {
        "model": llm_model,
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_text}],
        "temperature": 0.0,
        "max_tokens": 1000
    }
    try:
        resp = requests.post(f"{llm_url}/chat/completions", headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        clean_content = sanitize_text(content)
        if clean_content:
            logger.warning("generic_core_llm_non_json_response using direct_answer fallback")
            return {
                "intent": "direct_answer",
                "reply_text": clean_content,
                "extracted_entities": {},
                "requires_human": False,
            }
        return json.loads(content)
    except Exception as e:
        raise ValueError(f"LLM JSON Error: {e} - content: {content if 'content' in locals() else 'None'}")

def invoke_generic_llm(message_text: str, conversation: dict, memory: dict, history: list[dict]) -> dict:
    cfg = get_config()
    business_context = build_generic_business_context(message_text, cfg)
    
    # Minimize context parsing, formatting user messages
    recent = "\\n".join([f"{msg.get('direction', 'IN').upper()}: {msg.get('message_text', '')}" for msg in history[-10:]])
    
    # Exposing missing booking fields to explicitly direct the AI on what to ask if it proceeds to 'active_booking'
    missing = []
    if not known_requested_service(conversation, memory): missing.append("Hizmet Türü")
    if not conversation.get("lead_name"): missing.append("İsim Soyisim")
    if not conversation.get("phone"): missing.append("Telefon Numarası")
    if not conversation.get("requested_date") or not conversation.get("requested_time"): missing.append("Tarih ve Saat")
    
    today = datetime.date.today().strftime('%Y-%m-%d')
    system_prompt = f"""Sen {cfg.get('business_name')} firmasının Instagram DM asistanısın. Türkçe, doğal ve kısa yaz. BUGÜNÜN TARİHİ: {today}. Tarih gerekiyorsa YYYY-MM-DD hesapla.

KONUŞMA STİLİ:
- En son müşteri mesajını merkeze al; önce o mesaja doğrudan cevap ver.
- Önceki konuşmada zaten selamlaştıysanız tekrar "Merhaba/Selam" ile başlama.
- Instagram DM gibi kısa yaz: çoğu cevap 1-2 kısa cümle olsun.
- En fazla 1 net soru sor; birden fazla eksik bilgiyi aynı anda sorma.
- Müşteri açıkça istemedikçe randevu, ön görüşme, telefon veya tarih/saat isteme; booking akışını zorlama.
- Genel kurumsal tanıtım, hizmet kataloğu dökümü ve alakasız çapraz satış yapma.
- Sadece Business Context'teki bilgiye dayan; fiyat, süre, hizmet veya müsaitlik uydurma.
- Business Context'teki sunulmayan hizmetler bilgisini sadece kullanıcı doğrudan "siz X yapıyor musunuz/veriyor musunuz?" diye sorarsa kullan; müşteri kendi sektörünü söylüyorsa bu listeyi dışlama cevabına çevirme.
- Kullanıcı fiyat sorarsa ilgili hizmet biliniyorsa fiyatı doğrudan söyle; bilinmiyorsa tek soru ile hangi hizmet olduğunu sor.
- Kullanıcı küçük sohbet veya selam yazdıysa satışa geçmeden kısa ve insani cevap ver.
- Kullanıcı açıkça randevu/ön görüşme/planlama isterse randevu akışına gir ve aşağıdaki eksik alanlardan sadece ilkini sor.

RANDEVU AKIŞI:
Eğer son konuşmada veya hafızada bir hizmet zaten biliniyorsa (requested_service / selected_service / service_interest), booking opt-in geldiğinde bu hizmeti kullan; "hangi hizmeti araştırıyorsunuz?" diye tekrar sorma.
Şu an randevu için eksik olan kritik bilgiler: {', '.join(missing) if missing else 'YOK. Randevu Onaylanabilir.'}

İŞLETME BİLGİSİ (Business Context):
{business_context}

SON KONUŞMA GEÇMİŞİ:
{recent}

Müşterinin yeni mesajını incele. Oku ve aşağıdaki JSON formatına SIKI SIKIYA uygun bir yanıt dön:
{{
    "intent": "direct_answer" | "service_question" | "price_question" | "booking_request" | "active_booking" | "human_handoff" | "fallback",
    "reply_text": "Müşteriye yazacağın Türkçe doğal yanıt",
    "extracted_entities": {{
        "lead_name": "Eğer bu mesajda veya geçmişte müşterinin ismini bulduysan çıkar, yoksa null",
        "phone": "Eğer tam bir telefon numarası verildiyse çıkar, yoksa null",
        "requested_service": "Eğer config.services içinden biri istenmişse o hizmetin ismini yaz, yoksa null",
        "requested_date": "YYYY-MM-DD olarak tarih (varsa), yoksa null",
        "requested_time": "HH:MM olarak saat (varsa), yoksa null",
        "customer_goal": "Müşterinin elde etmek istediği amaç. (Yoksa null)"
    }},
    "requires_human": true | false
}}"""

    try:
        return call_llm_json(system_prompt, message_text)
    except Exception as e:
        logger.error(f"Generic engine LLM Error: {e}")
        return {
            "intent": "fallback",
            "reply_text": cfg.get("fallback_reply") or "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
            "extracted_entities": {},
            "requires_human": False
        }

def generic_quality_guard(reply: str, extracted: dict, memory: dict, cfg: dict, message_text: str | None = None) -> Tuple[str, Optional[str]]:
    # 1. Config Service Matching
    if extracted.get("requested_service"):
        valid_services = [s.get("name", "").lower() for s in cfg.get("service_catalog", [])] + [s.get("display", "").lower() for s in cfg.get("service_catalog", [])]
        matched = False
        requested_service = extracted["requested_service"].lower()
        for svc in valid_services:
            if svc and (svc in requested_service or requested_service in svc):
                matched = True
        if not matched:
            extracted["requested_service"] = None
            
    # 2. Prevent Booking confirmed without real fields locally
    if "Kaydınız oluşturuldu" in reply and (not memory.get("customer_phone") or not memory.get("requested_service")):
        return "İşlemlerinize devam edebilmem için lütfen bilgileri eksiksiz tamamlayalım.", "prevent_premature_confirm"

    cleaned_reply = strip_leading_greeting_for_non_greeting(message_text or "", reply)
    if cleaned_reply != reply:
        return cleaned_reply, "strip_repeated_greeting"
        
    return reply, None
