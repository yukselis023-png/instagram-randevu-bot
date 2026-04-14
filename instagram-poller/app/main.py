import json
import logging
import os
import signal
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import speech_recognition as sr
from instagrapi import Client
from instagrapi.exceptions import LoginRequired

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("instagram_poller")

IG_LOGIN_USERNAME = os.getenv("IG_LOGIN_USERNAME", "").strip()
IG_LOGIN_PASSWORD = os.getenv("IG_LOGIN_PASSWORD", "").strip()
BOOKING_API_BASE_URL = os.getenv("BOOKING_API_BASE_URL", "http://booking-api:8000").rstrip("/")
N8N_PROCESS_WEBHOOK_URL = os.getenv("N8N_PROCESS_WEBHOOK_URL", "").strip()
IG_POLL_INTERVAL_SECONDS = int(os.getenv("IG_POLL_INTERVAL_SECONDS", "3"))
IG_THREAD_FETCH_LIMIT = int(os.getenv("IG_THREAD_FETCH_LIMIT", "20"))
IG_MESSAGE_FETCH_LIMIT = int(os.getenv("IG_MESSAGE_FETCH_LIMIT", "10"))
IG_ACTIVE_THREAD_REFRESH_LIMIT = int(os.getenv("IG_ACTIVE_THREAD_REFRESH_LIMIT", "5"))
IG_PENDING_INBOX_ENABLED = os.getenv("IG_PENDING_INBOX_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
IG_PROXY = os.getenv("IG_PROXY", "").strip()
POLLER_REMINDERS_ENABLED = os.getenv("POLLER_REMINDERS_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
VOICE_TRANSCRIPTION_ENABLED = os.getenv("VOICE_TRANSCRIPTION_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
VOICE_TRANSCRIPTION_LANGUAGE = os.getenv("VOICE_TRANSCRIPTION_LANGUAGE", "tr-TR").strip() or "tr-TR"

DATA_DIR = Path(os.getenv("IG_DATA_DIR", "/app/data"))
SESSION_FILE = DATA_DIR / "session.json"
STATE_FILE = DATA_DIR / "state.json"
HEARTBEAT_FILE = DATA_DIR / "heartbeat.json"
MAX_TRACKED_MESSAGE_IDS = int(os.getenv("MAX_TRACKED_MESSAGE_IDS", "5000"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "90"))
VOICE_TMP_DIR = DATA_DIR / "voice-tmp"

RUNNING = True


def handle_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global RUNNING
    logger.info("Signal received: %s. Shutting down...", signum)
    RUNNING = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


class PollerState:
    def __init__(self, processed_message_ids: list[str] | None = None, bootstrapped_at: str | None = None) -> None:
        self.processed_message_ids = processed_message_ids or []
        self.bootstrapped_at = bootstrapped_at
        self._seen = set(self.processed_message_ids)

    def has(self, message_id: str) -> bool:
        return message_id in self._seen

    def add(self, message_id: str) -> None:
        if message_id in self._seen:
            return
        self.processed_message_ids.append(message_id)
        self._seen.add(message_id)
        if len(self.processed_message_ids) > MAX_TRACKED_MESSAGE_IDS:
            overflow = len(self.processed_message_ids) - MAX_TRACKED_MESSAGE_IDS
            removed = self.processed_message_ids[:overflow]
            self.processed_message_ids = self.processed_message_ids[overflow:]
            for item in removed:
                self._seen.discard(item)

    def mark_bootstrapped(self) -> None:
        self.bootstrapped_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "processed_message_ids": self.processed_message_ids,
            "bootstrapped_at": self.bootstrapped_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }



def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    VOICE_TMP_DIR.mkdir(parents=True, exist_ok=True)



def load_state() -> PollerState:
    ensure_data_dir()
    if not STATE_FILE.exists():
        return PollerState()
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return PollerState(
            processed_message_ids=[str(x) for x in data.get("processed_message_ids", [])],
            bootstrapped_at=data.get("bootstrapped_at"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read state file, starting fresh: %s", exc)
        return PollerState()



def save_state(state: PollerState) -> None:
    ensure_data_dir()
    tmp_file = STATE_FILE.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_file.replace(STATE_FILE)



def write_heartbeat(status: str, extra: dict[str, Any] | None = None) -> None:
    ensure_data_dir()
    payload: dict[str, Any] = {
        "status": status,
        "time": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload.update(extra)
    HEARTBEAT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): serialize_value(v) for k, v in value.items()}
    if hasattr(value, "dict"):
        try:
            return serialize_value(value.dict())
        except Exception:  # noqa: BLE001
            pass
    if hasattr(value, "model_dump"):
        try:
            return serialize_value(value.model_dump())
        except Exception:  # noqa: BLE001
            pass
    if hasattr(value, "__dict__"):
        data = {}
        for key, val in vars(value).items():
            if key.startswith("_"):
                continue
            data[key] = serialize_value(val)
        return data
    return str(value)



def build_client() -> Client:
    cl = Client()
    cl.delay_range = [1, 3]
    if IG_PROXY:
        cl.set_proxy(IG_PROXY)
    return cl



def login_client() -> Client:
    if not IG_LOGIN_USERNAME or not IG_LOGIN_PASSWORD:
        raise RuntimeError("IG_LOGIN_USERNAME / IG_LOGIN_PASSWORD missing")

    ensure_data_dir()
    cl = build_client()

    if SESSION_FILE.exists():
        try:
            session = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
            cl.set_settings(session)
            cl.login(IG_LOGIN_USERNAME, IG_LOGIN_PASSWORD)
            try:
                cl.get_timeline_feed()
            except LoginRequired:
                logger.warning("Session invalid, retrying with password login and preserved uuids")
                old_settings = cl.get_settings()
                uuids = old_settings.get("uuids", {})
                cl = build_client()
                if uuids:
                    cl.set_uuids(uuids)
                cl.login(IG_LOGIN_USERNAME, IG_LOGIN_PASSWORD)
            SESSION_FILE.write_text(json.dumps(cl.get_settings(), ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("Instagram login via saved session succeeded as %s (user_id=%s)", IG_LOGIN_USERNAME, cl.user_id)
            return cl
        except Exception as exc:  # noqa: BLE001
            logger.warning("Session login failed, will try fresh password login: %s", exc)

    cl = build_client()
    if not cl.login(IG_LOGIN_USERNAME, IG_LOGIN_PASSWORD):
        raise RuntimeError("Instagram login failed")
    SESSION_FILE.write_text(json.dumps(cl.get_settings(), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Instagram fresh login succeeded as %s (user_id=%s)", IG_LOGIN_USERNAME, cl.user_id)
    return cl



def fetch_raw_threads(cl: Client, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        response = cl.private_request(endpoint, params=params)
        return response.get("inbox", {}).get("threads", []) or []
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch %s: %s", endpoint, exc)
        return []



def extract_message_id(message: dict[str, Any]) -> str:
    return str(message.get("item_id") or message.get("id") or message.get("pk") or "")



def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:  # noqa: BLE001
        return 0



def thread_latest_item_ts(thread: dict[str, Any]) -> int:
    return max((as_int(item.get("timestamp")) for item in (thread.get("items", []) or [])), default=0)



def merge_thread_snapshots(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    current_items = current.get("items", []) or []
    candidate_items = candidate.get("items", []) or []
    merged_items: dict[str, dict[str, Any]] = {}

    for item in current_items + candidate_items:
        item_id = extract_message_id(item)
        key = item_id or f"fallback-{len(merged_items)}"
        previous = merged_items.get(key)
        if previous is None or as_int(item.get("timestamp")) >= as_int(previous.get("timestamp")):
            merged_items[key] = item

    current_score = (thread_latest_item_ts(current), as_int(current.get("last_activity_at")), len(current_items))
    candidate_score = (thread_latest_item_ts(candidate), as_int(candidate.get("last_activity_at")), len(candidate_items))
    preferred = candidate if candidate_score >= current_score else current
    other = current if preferred is candidate else candidate

    merged = dict(other)
    merged.update(preferred)
    merged["items"] = sorted(merged_items.values(), key=lambda item: as_int(item.get("timestamp")), reverse=True)
    if not merged.get("users") and other.get("users"):
        merged["users"] = other.get("users")
    if as_int(other.get("last_activity_at")) > as_int(merged.get("last_activity_at")):
        merged["last_activity_at"] = other.get("last_activity_at")
    return merged



def refresh_thread_if_stale(cl: Client, thread: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    thread_id = str(thread.get("thread_id") or thread.get("id") or thread.get("thread_v2_id") or "")
    if not thread_id:
        return thread

    latest_item_ts = thread_latest_item_ts(thread)
    last_activity_at = as_int(thread.get("last_activity_at"))
    if not force and latest_item_ts and last_activity_at and last_activity_at <= latest_item_ts:
        return thread

    try:
        params = {
            "visual_message_return_type": "unseen",
            "direction": "older",
            "seq_id": "40065",
            "limit": str(max(IG_MESSAGE_FETCH_LIMIT, 20)),
        }
        result = cl.private_request(f"direct_v2/threads/{thread_id}/", params=params)
        fresh_thread = result.get("thread") or {}
        if not fresh_thread:
            return thread
        merged = merge_thread_snapshots(thread, fresh_thread)
        logger.info(
            "Refreshed thread thread_id=%s force=%s latest_item_ts=%s last_activity_at=%s merged_items=%s",
            thread_id,
            force,
            latest_item_ts,
            last_activity_at,
            len(merged.get("items", []) or []),
        )
        return merged
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to refresh thread_id=%s force=%s: %s", thread_id, force, exc)
        return thread



def fetch_threads(cl: Client) -> list[dict[str, Any]]:
    threads_by_id: dict[str, dict[str, Any]] = {}

    def add_thread_list(items: list[dict[str, Any]], source: str) -> None:
        for thread in items or []:
            thread_id = str(thread.get("thread_id") or thread.get("id") or thread.get("thread_v2_id") or "")
            if not thread_id:
                continue
            existing = threads_by_id.get(thread_id)
            if existing is None:
                threads_by_id[thread_id] = thread
                continue
            threads_by_id[thread_id] = merge_thread_snapshots(existing, thread)
            logger.info("Merged duplicate thread snapshot thread_id=%s source=%s", thread_id, source)

    unread_threads = fetch_raw_threads(
        cl,
        "direct_v2/inbox/",
        {
            "visual_message_return_type": "unseen",
            "thread_message_limit": str(IG_MESSAGE_FETCH_LIMIT),
            "persistentBadging": "true",
            "limit": str(IG_THREAD_FETCH_LIMIT),
            "is_prefetching": "false",
            "selected_filter": "unread",
        },
    )
    add_thread_list(unread_threads, "unread")

    if IG_PENDING_INBOX_ENABLED:
        pending_threads = fetch_raw_threads(
            cl,
            "direct_v2/pending_inbox/",
            {
                "visual_message_return_type": "unseen",
                "persistentBadging": "true",
                "is_prefetching": "false",
            },
        )
        add_thread_list(pending_threads, "pending")

    if not threads_by_id:
        recent_limit = max(3, min(IG_THREAD_FETCH_LIMIT, 5))
        recent_threads = fetch_raw_threads(
            cl,
            "direct_v2/inbox/",
            {
                "visual_message_return_type": "unseen",
                "thread_message_limit": str(IG_MESSAGE_FETCH_LIMIT),
                "persistentBadging": "true",
                "limit": str(recent_limit),
                "is_prefetching": "false",
            },
        )
        add_thread_list(recent_threads, "recent")

    threads = list(threads_by_id.values())
    threads.sort(key=lambda thread: max(thread_latest_item_ts(thread), as_int(thread.get("last_activity_at"))), reverse=True)

    refreshed_threads: list[dict[str, Any]] = []
    refresh_limit = 1 if unread_threads or threads_by_id else max(1, min(IG_ACTIVE_THREAD_REFRESH_LIMIT, 2))
    for index, thread in enumerate(threads):
        should_force_refresh = index < refresh_limit
        refreshed_threads.append(refresh_thread_if_stale(cl, thread, force=should_force_refresh))

    refreshed_threads.sort(key=lambda thread: max(thread_latest_item_ts(thread), as_int(thread.get("last_activity_at"))), reverse=True)
    return refreshed_threads


def bootstrap_existing_messages(cl: Client, state: PollerState) -> None:
    if state.bootstrapped_at:
        return
    threads = fetch_threads(cl)
    bootstrapped = 0
    for thread in threads:
        for message in get_thread_messages(thread):
            message_id = str(message.get("item_id") or message.get("id") or message.get("pk") or "")
            if not message_id:
                continue
            state.add(message_id)
            bootstrapped += 1
    state.mark_bootstrapped()
    save_state(state)
    logger.info("Bootstrap complete, existing_messages_marked=%s", bootstrapped)



def get_thread_messages(thread: dict[str, Any]) -> list[dict[str, Any]]:
    items = thread.get("items", []) or []

    def sort_key(item: dict[str, Any]) -> int:
        raw_value = item.get("timestamp") or 0
        try:
            return int(raw_value)
        except Exception:  # noqa: BLE001
            return 0

    return sorted(items, key=sort_key)



def normalize_processing_response(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            if "json" in first and isinstance(first["json"], dict):
                return first["json"]
            return first
    return {}


def should_fallback_processing_result(result: dict[str, Any]) -> bool:
    if not result:
        return True
    if bool(result.get("error")):
        return True
    reply_text = str(result.get("reply_text") or "").strip()
    should_reply = result.get("should_reply")
    if should_reply is False and not reply_text:
        return True
    return False


def extract_voice_audio_url(message: dict[str, Any]) -> str | None:
    candidates = [
        (((message.get("voice_media") or {}).get("media") or {}).get("audio") or {}).get("audio_src"),
        (((message.get("voice_media") or {}).get("media") or {}).get("audio") or {}).get("audio_url"),
        ((message.get("voice_media") or {}).get("media") or {}).get("audio_url"),
        ((message.get("media") or {}).get("audio") or {}).get("audio_src"),
        ((message.get("media") or {}).get("audio") or {}).get("audio_url"),
        ((message.get("media") or {}).get("audio_url")),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return None


def transcribe_voice_message(cl: Client, audio_url: str) -> tuple[str | None, str | None]:
    if not VOICE_TRANSCRIPTION_ENABLED:
        return None, "voice_transcription_disabled"

    temp_path: Path | None = None
    wav_path: Path | None = None
    try:
        response = None
        try:
            response = requests.get(audio_url, timeout=REQUEST_TIMEOUT_SECONDS)
        except Exception:  # noqa: BLE001
            response = None
        if response is None or not response.ok:
            private_session = getattr(getattr(cl, "private", None), "session", None)
            if private_session is not None:
                response = private_session.get(audio_url, timeout=REQUEST_TIMEOUT_SECONDS)
        if response is None:
            return None, "voice_download_failed"
        response.raise_for_status()
        suffix = Path(audio_url.split("?")[0]).suffix or ".mp4"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=VOICE_TMP_DIR) as tmp:
            tmp.write(response.content)
            temp_path = Path(tmp.name)
        wav_path = temp_path.with_suffix('.wav')
        import subprocess
        proc = subprocess.run([
            'ffmpeg', '-y', '-i', str(temp_path), '-ar', '16000', '-ac', '1', str(wav_path)
        ], capture_output=True, text=True)
        if proc.returncode != 0 or not wav_path.exists():
            return None, f'voice_ffmpeg_failed:{proc.stderr[-200:]}'
        recognizer = sr.Recognizer()
        with sr.AudioFile(str(wav_path)) as source:
            audio = recognizer.record(source)

        errors: list[str] = []

        try:
            transcript = recognizer.recognize_google(audio, language=VOICE_TRANSCRIPTION_LANGUAGE).strip()
            if transcript:
                return transcript, None
            errors.append('google_empty')
        except Exception as exc:  # noqa: BLE001
            errors.append(f'google:{exc}')

        return None, 'voice_transcript_failed:' + ' | '.join(errors[-2:])
    except Exception as exc:  # noqa: BLE001
        logger.warning("Voice transcription failed: %s", exc)
        return None, str(exc)
    finally:
        for path in [temp_path, wav_path]:
            if path and path.exists():
                try:
                    path.unlink()
                except Exception:  # noqa: BLE001
                    pass


def build_voice_fallback_reply() -> str:
    return "Sesli mesajınızı aldım ama şu an metne dökemediğim için yanlış yönlendirmek istemem. Müsaitseniz aynı şeyi kısa bir yazı olarak da paylaşır mısınız?"


def report_voice_fallback(sender_id: str, username: str | None, fallback_reply: str, raw_event: dict[str, Any], transcription_error: str | None = None) -> dict[str, Any]:
    try:
        response = requests.post(
            f"{BOOKING_API_BASE_URL}/internal/messages/voice-fallback",
            json={
                "sender_id": sender_id,
                "instagram_username": username,
                "fallback_reply": fallback_reply,
                "transcription_error": transcription_error,
                "raw_event": raw_event,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Voice fallback reporting failed sender_id=%s: %s", sender_id, exc)
        return {}


def claim_due_morning_reminders() -> list[dict[str, Any]]:
    if not POLLER_REMINDERS_ENABLED:
        return []
    try:
        response = requests.get(
            f"{BOOKING_API_BASE_URL}/internal/reminders/morning/claim",
            params={"limit": 10},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = normalize_processing_response(response.json())
        reminders = payload.get("reminders") if isinstance(payload, dict) else None
        return reminders if isinstance(reminders, list) else []
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to claim morning reminders: %s", exc)
        return []


def mark_due_morning_reminder(reminder_id: int, claim_token: str, sent: bool, error: str | None = None) -> None:
    try:
        response = requests.post(
            f"{BOOKING_API_BASE_URL}/internal/reminders/morning/mark",
            json={
                "reminder_id": reminder_id,
                "claim_token": claim_token,
                "sent": sent,
                "error": error,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to mark morning reminder id=%s sent=%s error=%s: %s", reminder_id, sent, error, exc)


def process_due_morning_reminders(cl: Client) -> None:
    reminders = claim_due_morning_reminders()
    for reminder in reminders:
        reminder_id = int(reminder.get("reminder_id") or 0)
        sender_id = str(reminder.get("instagram_user_id") or "").strip()
        claim_token = str(reminder.get("claim_token") or "").strip()
        reminder_text = str(reminder.get("reminder_text") or "").strip()
        if not reminder_id or not sender_id or not claim_token or not reminder_text:
            continue
        try:
            cl.direct_send(reminder_text, user_ids=[int(sender_id)])
            logger.info("Morning reminder sent reminder_id=%s sender_id=%s", reminder_id, sender_id)
            mark_due_morning_reminder(reminder_id, claim_token, True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Morning reminder send failed reminder_id=%s sender_id=%s: %s", reminder_id, sender_id, exc)
            mark_due_morning_reminder(reminder_id, claim_token, False, str(exc))


def post_to_processing_backend(sender_id: str, username: str | None, message_text: str, raw_event: dict[str, Any]) -> dict[str, Any]:
    trace_id = str(raw_event.get("trace_id") or raw_event.get("message_id") or sender_id)
    payload = {
        "sender_id": sender_id,
        "instagram_username": username,
        "message_text": message_text,
        "raw_event": raw_event,
        "trace_id": trace_id,
    }

    if N8N_PROCESS_WEBHOOK_URL:
        try:
            logger.info("poller_post_n8n trace_id=%s sender_id=%s webhook=%s", trace_id, sender_id, N8N_PROCESS_WEBHOOK_URL)
            response = requests.post(
                N8N_PROCESS_WEBHOOK_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            n8n_result = normalize_processing_response(response.json())
            logger.info("poller_n8n_response trace_id=%s sender_id=%s status=%s should_reply=%s has_reply=%s", trace_id, sender_id, response.status_code, n8n_result.get("should_reply"), bool(str(n8n_result.get("reply_text") or "").strip()))
            if not should_fallback_processing_result(n8n_result):
                return n8n_result
            logger.warning("n8n returned no AI reply trace_id=%s sender_id=%s, trying booking-api directly", trace_id, sender_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("n8n processing failed trace_id=%s sender_id=%s, trying booking-api directly without local fallback: %s", trace_id, sender_id, exc)

    try:
        logger.info("poller_post_api trace_id=%s sender_id=%s api=%s", trace_id, sender_id, f"{BOOKING_API_BASE_URL}/api/process-instagram-message")
        response = requests.post(
            f"{BOOKING_API_BASE_URL}/api/process-instagram-message",
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        api_result = normalize_processing_response(response.json())
        logger.info("poller_api_response trace_id=%s sender_id=%s status=%s should_reply=%s has_reply=%s", trace_id, sender_id, response.status_code, api_result.get("should_reply"), bool(str(api_result.get("reply_text") or "").strip()))
        if api_result:
            return api_result
        logger.warning("booking-api returned empty payload trace_id=%s sender_id=%s", trace_id, sender_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("booking-api processing failed trace_id=%s sender_id=%s: %s", trace_id, sender_id, exc)

    return {
        "should_reply": False,
        "reply_text": None,
        "handoff": False,
        "conversation_state": "ai_unavailable",
        "normalized": {},
        "decision_path": ["ai_unavailable"],
    }



def process_message(cl: Client, state: PollerState, thread: dict[str, Any], message: dict[str, Any]) -> None:
    my_user_id = str(cl.user_id)
    message_id = extract_message_id(message)
    sender_id = str(message.get("user_id") or "")
    thread_id = str(thread.get("thread_id") or thread.get("id") or thread.get("thread_v2_id") or "")
    item_type = str(message.get("item_type") or "")
    text = (message.get("text") or "").strip()

    if not message_id:
        logger.info("Skipping message without id thread_id=%s sender_id=%s", thread_id, sender_id)
        return
    if state.has(message_id):
        logger.info("Skipping duplicate message_id=%s thread_id=%s sender_id=%s", message_id, thread_id, sender_id)
        return
    if sender_id == my_user_id:
        logger.info("Skipping self message_id=%s thread_id=%s", message_id, thread_id)
        state.add(message_id)
        save_state(state)
        return

    users = thread.get("users", []) or []
    other_user = None
    for user in users:
        user_pk = str(user.get("pk") or "")
        if user_pk and user_pk != my_user_id:
            other_user = user
            break
    username = other_user.get("username") if other_user else None

    processed_text = text
    transcription_error = None
    source_kind = "text"

    if item_type != "text":
        audio_url = extract_voice_audio_url(message)
        if audio_url:
            source_kind = "voice"
            processed_text, transcription_error = transcribe_voice_message(cl, audio_url)
            if processed_text:
                logger.info("Voice message transcribed message_id=%s sender_id=%s transcript_length=%s", message_id, sender_id, len(processed_text))
            else:
                fallback_reply = build_voice_fallback_reply()
                fallback_event = {
                    "source": "instagrapi_private_api",
                    "thread_id": thread_id,
                    "message_id": message_id,
                    "sender_id": sender_id,
                    "item_type": item_type,
                    "message_source": "voice",
                    "timestamp": serialize_value(message.get("timestamp")),
                    "thread": serialize_value(thread),
                    "message": serialize_value(message),
                    "transcription_error": transcription_error,
                }
                fallback_result = report_voice_fallback(sender_id, username, fallback_reply, fallback_event, transcription_error)
                reply_to_send = str(fallback_result.get("reply_text") or fallback_reply).strip()
                should_reply = bool(fallback_result.get("should_reply", True))
                if should_reply and reply_to_send:
                    try:
                        cl.direct_answer(int(thread_id), reply_to_send)
                        logger.info("Voice fallback reply sent sender_id=%s thread_id=%s", sender_id, thread_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Voice fallback send failed sender_id=%s thread_id=%s: %s", sender_id, thread_id, exc)
                else:
                    logger.info("Voice fallback already handled for sender_id=%s message_id=%s", sender_id, message_id)
                state.add(message_id)
                save_state(state)
                return
        else:
            logger.info("Skipping non-text message %s in thread %s (type=%s)", message_id, thread_id, item_type)
            state.add(message_id)
            save_state(state)
            return

    if not processed_text:
        logger.info("Skipping empty processed message %s in thread %s", message_id, thread_id)
        state.add(message_id)
        save_state(state)
        return

    trace_id = f"igdm:{sender_id}:{message_id}"
    raw_event = {
        "source": "instagrapi_private_api",
        "thread_id": thread_id,
        "message_id": message_id,
        "trace_id": trace_id,
        "sender_id": sender_id,
        "item_type": item_type,
        "message_source": source_kind,
        "timestamp": serialize_value(message.get("timestamp")),
        "thread": serialize_value(thread),
        "message": serialize_value(message),
    }
    if source_kind == "voice":
        raw_event["transcribed_text"] = processed_text
        if transcription_error:
            raw_event["transcription_error"] = transcription_error

    backend_label = N8N_PROCESS_WEBHOOK_URL or f"{BOOKING_API_BASE_URL}/api/process-instagram-message"
    logger.info("poller_inbound_dm trace_id=%s message_id=%s sender_id=%s username=%s via=%s source=%s text=%s", trace_id, message_id, sender_id, username, backend_label, source_kind, processed_text[:160])
    result = post_to_processing_backend(sender_id, username, processed_text, raw_event)
    should_reply = bool(result.get("should_reply"))
    reply_text = (result.get("reply_text") or "").strip()

    if should_reply and reply_text:
        try:
            cl.direct_answer(int(thread_id), reply_text)
            logger.info("poller_reply_sent trace_id=%s sender_id=%s thread_id=%s", trace_id, sender_id, thread_id)
        except Exception as exc:
            logger.warning("poller_reply_failed trace_id=%s sender_id=%s thread_id=%s error=%s", trace_id, sender_id, thread_id, exc)
    else:
        logger.info("poller_no_reply trace_id=%s message_id=%s sender_id=%s decision_path=%s", trace_id, message_id, sender_id, result.get("decision_path"))

    state.add(message_id)
    save_state(state)



def main() -> int:
    ensure_data_dir()
    state = load_state()
    write_heartbeat("starting")

    cl: Client | None = None
    while RUNNING:
        try:
            if cl is None:
                cl = login_client()
                bootstrap_existing_messages(cl, state)

            process_due_morning_reminders(cl)
            threads = fetch_threads(cl)
            write_heartbeat("running", {"thread_count": len(threads), "instagram_user_id": str(cl.user_id)})

            processed_now = 0
            for thread in threads:
                if not RUNNING:
                    break
                messages = get_thread_messages(thread)
                for message in messages:
                    process_message(cl, state, thread, message)
                    processed_now += 1

            if processed_now:
                logger.info("Cycle complete, processed_messages=%s", processed_now)

            time.sleep(IG_POLL_INTERVAL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Poller loop error: %s", exc)
            write_heartbeat("error", {"error": str(exc)})
            cl = None
            time.sleep(max(IG_POLL_INTERVAL_SECONDS, 10))

    write_heartbeat("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
