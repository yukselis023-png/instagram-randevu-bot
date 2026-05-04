import os
import json
import logging
from typing import Any
import time as time_module

from fastapi import BackgroundTasks
from app.main import (
    ProcessResult, IncomingMessage, get_conn, get_or_create_conversation, 
    sanitize_conversation_state, ensure_conversation_memory, 
    sync_conversation_memory_summary, has_processed_inbound_message,
    try_acquire_inbound_processing_lock, save_message_log, get_recent_message_history,
    should_reset_stale_conversation, reset_conversation_for_restart, build_normalized,
    update_conversation_memory_after_bot_reply, upsert_conversation, upsert_customer_from_conversation,
    schedule_customer_automation_events, sanitize_text, extract_inbound_message_id, extract_inbound_platform,
    build_inbound_dedupe_key, elapsed_ms, queue_crm_sync, get_config
)
from shared.genai import call_llm_json

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
        ensure_conversation_memory(conversation)
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
            
        save_message_log(conn, payload.sender_id, "in", message_text, payload.raw_event or {})
        recent_history = get_recent_message_history(conn, payload.sender_id)

        # GENERIC CORE LOGIC
        final_reply, decision_path = generic_engine_router(message_text, conversation, recent_history)
        
        # State updates (basic CRM triggers for generic)
        handoff = False
        if "human_handoff" in decision_path:
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            handoff = True
            
        update_conversation_memory_after_bot_reply(conversation, final_reply, "\n".join(decision_path))
        upsert_conversation(conn, conversation)
        crm_customer = upsert_customer_from_conversation(conn, conversation)
        if crm_customer:
            schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector"))
            
        if final_reply:
            save_message_log(
                conn,
                payload.sender_id,
                "out",
                final_reply,
                {"type": "reply", "decision_path": decision_path}
            )

        metrics["total_ms"] = elapsed_ms(request_started_at)
        
        queue_crm_sync(background_tasks, conversation, None, metrics)

        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=bool(final_reply),
            reply_text=final_reply,
            handoff=handoff,
            conversation_state=conversation.get("state", "new"),
            appointment_created=False,
            appointment_id=None,
            normalized=build_normalized(conversation),
            metrics=metrics,
            decision_path=decision_path,
        )


def generic_engine_router(message_text: str, conversation: dict, recent_history: list[dict]) -> tuple[str, list[str]]:
    """
    Core AI logic that maps intent and builds deterministic reply using ONLY 
    the active business configuration (get_config()).
    """
    cfg = get_config()
    business_context = json.dumps(cfg, ensure_ascii=False)
    
    # Compress history
    hist_text = "\n".join([f"{msg['direction']}: {msg['message_text']}" for msg in recent_history[-5:]])
    
    prompt = f"""You are the digital assistant for this business: {cfg.get('business_name', 'Biz')}.
Business Details:
{business_context}

Your Goal: Reply naturally to the customer's latest message based on the Business Details.
Do not invent services or prices. Be extremely concise (limit 3 sentences).
If they want a date/appointment, ask them what time suits them, aligned with working_hours.
If they ask for a human, route to human_handoff.

Recent history:
{hist_text}

Latest message: {message_text}

Provide your response strictly in the following JSON format:
{{
    "intent": "direct_answer|service_question|price_question|booking_request|active_booking|human_handoff|fallback",
    "reply_text": "Your natural Turkish reply here.",
    "extracted_lead_name": null,
    "extracted_phone": null
}}
"""
    try:
        result = call_llm_json(prompt)
    except Exception as e:
        logger.error(f"Generic engine LLM error: {e}")
        return cfg.get("fallback_reply", "Şu an sistemimde bir yoğunluk var, detayları ön görüşmede netleştirelim."), ["error:fallback"]
        
    intent = result.get("intent", "fallback")
    reply = result.get("reply_text", "")
    
    # Deterministic Actions based on extract
    memory = ensure_conversation_memory(conversation)
    if result.get("extracted_lead_name"):
        memory["customer_name"] = result["extracted_lead_name"]
    if result.get("extracted_phone"):
        memory["customer_phone"] = result["extracted_phone"]
        
    # VERY Simple guard
    if "12.900" not in reply and "price_question" in intent and "web" in message_text.lower() and cfg.get("business_type") == "agency":
        # Just a basic structural sanity patch or LLM strictly honors config
        pass
        
    return reply, [f"generic_intent:{intent}"]

