"""Auto follow-up: re-engage customers who stopped responding.

Runs as background task:
  1. Query conversations with last_message > N hours
  2. Skip if already has appointment or handoff
  3. Send configured follow-up message
  4. Log to message history
"""
from __future__ import annotations
import json, logging, os
from typing import Any
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("followup")

DEFAULT_FOLLOWUP_DELAY_HOURS = int(os.getenv("FOLLOWUP_DELAY_HOURS", "24"))
DEFAULT_FOLLOWUP_MESSAGE = os.getenv(
    "FOLLOWUP_MESSAGE",
    "Merhaba, hala düşünüyor musunuz? Size yardımcı olabileceğim başka bir konu var mı?",
)
FOLLOWUP_COOLDOWN_HOURS = 72  # Don't follow up again within 72h

def find_conversations_due_followup(
    conn: Any,
    tenant_slug: str = None,
    delay_hours: int = DEFAULT_FOLLOWUP_DELAY_HOURS,
) -> list[dict[str, Any]]:
    """Find conversations that need follow-up.
    
    Criteria:
    - state NOT in (completed, handoff, cancelled)
    - no appointment_id
    - last customer message > delay_hours ago
    - no follow-up sent in last FOLLOWUP_COOLDOWN_HOURS
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=delay_hours)
        cooldown = datetime.now(timezone.utc) - timedelta(hours=FOLLOWUP_COOLDOWN_HOURS)
        
        query = """
            SELECT id, instagram_user_id, instagram_username, full_name, state, 
                   last_customer_message, memory_state, created_at
            FROM conversations
            WHERE state NOT IN ('completed', 'handoff', 'cancelled')
              AND appointment_id IS NULL
              AND updated_at < %s
              AND (memory_state->>'last_followup_at' IS NULL 
                   OR (memory_state->>'last_followup_at')::timestamptz < %s)
        """
        params = [cutoff, cooldown]
        
        if tenant_slug:
            query += " AND instagram_username LIKE %s"
            params.append(f"{tenant_slug}%")
        
        query += " ORDER BY updated_at ASC LIMIT 50"
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    except Exception as exc:
        logger.error("followup_query_error %s", exc)
        return []

def send_followup(
    conn: Any,
    conversation: dict[str, Any],
    reply_fn: callable,
    message: str = DEFAULT_FOLLOWUP_MESSAGE,
) -> bool:
    """Send follow-up message to customer."""
    sender_id = conversation.get("instagram_user_id")
    if not sender_id:
        return False
    
    try:
        # Mark followup time first
        memory = conversation.get("memory_state") or {}
        if isinstance(memory, str):
            try:
                memory = json.loads(memory)
            except Exception:
                memory = {}
        memory["last_followup_at"] = datetime.now(timezone.utc).isoformat()
        memory["followup_count"] = memory.get("followup_count", 0) + 1
        
        # Update conversation memory
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE conversations SET memory_state = %s, updated_at = NOW() WHERE id = %s",
                (json.dumps(memory), conversation["id"]),
            )
        conn.commit()
        
        # Send message via the reply function
        reply_fn(sender_id, message)
        logger.info("followup_sent user=%s message=%s", sender_id, message[:50])
        return True
    except Exception as exc:
        logger.error("followup_send_error user=%s error=%s", sender_id, exc)
        return False

def run_followup_cycle(conn: Any, reply_fn: callable, tenant_slug: str = None) -> dict[str, int]:
    """Full follow-up cycle: find → send. Returns stats."""
    conversations = find_conversations_due_followup(conn, tenant_slug)
    sent = 0
    for conv in conversations[:20]:  # Max 20 per cycle
        if send_followup(conn, conv, reply_fn):
            sent += 1
    return {"checked": len(conversations), "sent": sent}
