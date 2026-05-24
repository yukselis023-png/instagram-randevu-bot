"""Web Chat backend: handles embedded chat widget messages.

Architecture:
  <script src="https://api.doeldigital.com/webchat/widget.js?tenant=X"></script>
  → widget connects via WebSocket or POST
  → same core_engine pipeline
  → response streamed back
"""
from __future__ import annotations
import json, logging, uuid, re
from typing import Any, Callable
from datetime import datetime, timezone

logger = logging.getLogger("webchat")

# ── In-memory session store ─────────────────────────────────────────
# Simple dict; for production use Redis
_webchat_sessions: dict[str, dict[str, Any]] = {}

def get_or_create_session(session_id: str | None, tenant_slug: str) -> tuple[str, dict[str, Any]]:
    if not session_id:
        session_id = f"wc:{uuid.uuid4().hex[:12]}"
    if session_id not in _webchat_sessions:
        _webchat_sessions[session_id] = {
            "session_id": session_id,
            "tenant_slug": tenant_slug,
            "messages": [],
            "conversation": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    return session_id, _webchat_sessions[session_id]

def handle_webchat_message(
    session_id: str | None,
    tenant_slug: str,
    message_text: str,
    process_fn: Callable,
    background_tasks: Any,
    full_name: str | None = None,
    phone: str | None = None,
) -> dict[str, Any]:
    """Process a web chat message and return reply."""
    session_id, session = get_or_create_session(session_id, tenant_slug)
    sender_id = session_id  # Use session_id as sender_id

    # Attach name/phone if provided
    if full_name:
        session["full_name"] = full_name
    if phone:
        session["phone"] = phone

    message_id = f"wc:{uuid.uuid4().hex[:16]}"

    # Wrap as IncomingMessage-like
    class FakePayload:
        def __init__(self):
            self.sender_id = sender_id
            self.instagram_username = f"webchat_{tenant_slug}"
            self.message_text = message_text
            self.message_id = message_id
            self.raw_event = {"id": message_id, "source": "webchat", "tenant": tenant_slug}

    payload = FakePayload()
    try:
        result = process_fn(payload, background_tasks)
    except Exception as exc:
        logger.error("webchat_process_error session=%s error=%s", session_id, exc)
        result = None

    reply_text = ""
    appointment_created = False
    appointment_id = None
    if result:
        reply_text = getattr(result, "reply_text", "") or ""
        appointment_created = bool(getattr(result, "appointment_created", False))
        appointment_id = getattr(result, "appointment_id", None)
        conv_state = getattr(result, "conversation_state", "new")

    # Store in session
    session["messages"].append({"role": "user", "text": message_text})
    session["messages"].append({"role": "bot", "text": reply_text})
    if len(session["messages"]) > 50:
        session["messages"] = session["messages"][-50:]

    return {
        "session_id": session_id,
        "reply_text": reply_text,
        "appointment_created": appointment_created,
        "appointment_id": appointment_id,
        "full_name": session.get("full_name"),
        "phone": session.get("phone"),
    }

def get_session_history(session_id: str) -> dict[str, Any] | None:
    """Get full session data."""
    return _webchat_sessions.get(session_id)

def clear_stale_sessions(max_age_hours: int = 24):
    """Remove sessions older than max_age_hours."""
    now = datetime.now(timezone.utc)
    stale = []
    for sid, session in _webchat_sessions.items():
        created = session.get("created_at", "")
        try:
            dt = datetime.fromisoformat(created)
            if (now - dt).total_seconds() > max_age_hours * 3600:
                stale.append(sid)
        except Exception:
            stale.append(sid)
    for sid in stale:
        _webchat_sessions.pop(sid, None)
