"""Web Chat backend: handles embedded chat widget messages.

PostgreSQL-backed session store (no Redis dependency).
"""
from __future__ import annotations
import json, logging, uuid
from typing import Any, Callable
from datetime import datetime, timezone

logger = logging.getLogger("webchat")

TABLE = "webchat_sessions"


def ensure_table(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                session_id TEXT PRIMARY KEY,
                tenant_slug TEXT NOT NULL DEFAULT 'default',
                messages JSONB NOT NULL DEFAULT '[]'::jsonb,
                full_name TEXT, phone TEXT,
                conversation JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_tenant ON {TABLE}(tenant_slug)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_updated ON {TABLE}(updated_at)")


def get_or_create_session(conn: Any, session_id: str | None, tenant_slug: str) -> tuple[str, dict]:
    if not session_id:
        session_id = f"wc:{uuid.uuid4().hex[:12]}"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {TABLE} WHERE session_id = %s", (session_id,)
        )
        row = cur.fetchone()
        if row:
            s = {
                "session_id": row["session_id"],
                "tenant_slug": row["tenant_slug"],
                "messages": row["messages"] or [],
                "full_name": row["full_name"],
                "phone": row["phone"],
                "conversation": row["conversation"],
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
            }
        else:
            s = {"session_id": session_id, "tenant_slug": tenant_slug, "messages": [],
                 "full_name": None, "phone": None, "conversation": None,
                 "created_at": datetime.now(timezone.utc).isoformat(), "updated_at": None}
            cur.execute(
                f"INSERT INTO {TABLE} (session_id, tenant_slug) VALUES (%s, %s)",
                (session_id, tenant_slug),
            )
    return session_id, s


def _persist(conn: Any, s: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {TABLE} SET messages=%s, full_name=%s, phone=%s, conversation=%s, updated_at=NOW() "
            f"WHERE session_id=%s",
            (json.dumps(s.get("messages", [])), s.get("full_name"), s.get("phone"),
             json.dumps(s["conversation"]) if s.get("conversation") else None, s["session_id"]),
        )


def handle_webchat_message(
    session_id: str | None, tenant_slug: str, message_text: str,
    process_fn: Callable, background_tasks: Any,
    full_name: str | None = None, phone: str | None = None,
    conn: Any = None,
) -> dict:
    if not conn:
        from app import main as _m
        conn = _m.get_conn()
    session_id, session = get_or_create_session(conn, session_id, tenant_slug)
    if full_name:
        session["full_name"] = full_name
    if phone:
        session["phone"] = phone

    message_id = f"wc:{uuid.uuid4().hex[:16]}"

    class FakePayload:
        def __init__(self):
            self.sender_id = session_id
            self.instagram_username = f"webchat_{tenant_slug}"
            self.message_text = message_text
            self.message_id = message_id
            self.trace_id = message_id
            self.recipient_id = None
            self.raw_event = {"id": message_id, "source": "webchat", "tenant": tenant_slug, "platform": "webchat"}

    try:
        result = process_fn(FakePayload(), background_tasks)
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

    session["messages"].append({"role": "user", "text": message_text})
    session["messages"].append({"role": "bot", "text": reply_text})
    if len(session["messages"]) > 50:
        session["messages"] = session["messages"][-50:]

    try:
        _persist(conn, session)
    except Exception as exc:
        logger.warning("webchat_persist_error %s", exc)

    return {
        "session_id": session_id,
        "reply_text": reply_text,
        "appointment_created": appointment_created,
        "appointment_id": appointment_id,
        "full_name": session.get("full_name"),
        "phone": session.get("phone"),
    }


def get_session_history(session_id: str, conn: Any = None) -> dict | None:
    if not conn:
        from app import main as _m
        conn = _m.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {TABLE} WHERE session_id = %s", (session_id,))
        row = cur.fetchone()
        if row:
            return {"session_id": row["session_id"], "tenant_slug": row["tenant_slug"],
                    "messages": row["messages"] or [], "full_name": row["full_name"],
                    "phone": row["phone"], "conversation": row["conversation"],
                    "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                    "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None}
    return None


def clear_stale_sessions(conn: Any = None, max_age_hours: int = 48):
    if not conn:
        from app import main as _m
        conn = _m.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {TABLE} WHERE updated_at < NOW() - INTERVAL '%s hours'", (max_age_hours,))
        d = cur.rowcount
        if d:
            logger.info("webchat_cleared %d stale sessions", d)
