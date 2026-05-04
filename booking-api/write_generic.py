import codecs
content = r'''import os
import json
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
    build_inbound_dedupe_key, elapsed_ms, queue_crm_sync, get_config, call_llm_content
)

logger = logging.getLogger(__name__)

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

        # 2. STATE & CRM DETERMINISTIC LAYER
        handoff = False
        if intent == "human_handoff" or result_dict.get("requires_human"):
            decision_path.append("action:handoff")
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["appointment_status"] = "handoff"
            handoff = True

        # Sync deterministic entities immediately
        if extracted.get("lead_name"):
            conversation["lead_name"] = extracted["lead_name"]
            decision_path.append("extracted:name")
        if extracted.get("phone"):
            conversation["phone"] = extracted["phone"]
            decision_path.append("extracted:phone")
        if extracted.get("requested_service"):
            memory["requested_service"] = extracted["requested_service"]
            decision_path.append("extracted:service")
        if extracted.get("requested_date"):
            conversation["requested_date"] = extracted["requested_date"]
            decision_path.append("extracted:date")
        if extracted.get("requested_time"):
            conversation["requested_time"] = extracted["requested_time"]
            decision_path.append("extracted:time")
        if extracted.get("customer_goal"):
            memory["customer_goal"] = extracted["customer_goal"]

        conversation["memory_state"] = memory

        # 3. BOOKING FINITE STATE MACHINE (Only if booking intent or inside active flow)
        appointment_created = False
        curr_state = conversation.get("state", "new")
        
        if not handoff and (intent in ["booking_request", "active_booking"] or curr_state.startswith("collect_")):
            has_service = bool(memory.get("requested_service"))
            has_phone = bool(conversation.get("phone"))
            has_name = bool(conversation.get("lead_name"))
            has_date = bool(conversation.get("requested_date"))
            has_time = bool(conversation.get("requested_time"))

            # Progress the booking funnel deterministically
            if not has_service:
                conversation["state"] = "collect_service"
            elif not has_phone:
                conversation["state"] = "collect_phone"
            elif not has_name:
                conversation["state"] = "collect_name"
            elif not has_date or not has_time:
                conversation["state"] = "collect_datetime"
            else:
                conversation["state"] = "completed"
                conversation["appointment_status"] = "confirmed"
                appointment_created = True
                decision_path.append("action:appointment_confirmed")

        # 4. FINAL QUALITY GUARD (Post FSM Check)
        reply_text, guard_label = generic_quality_guard(reply_text, extracted, memory, get_config())
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
            appointment_id=None,
            normalized=build_normalized(conversation),
      
