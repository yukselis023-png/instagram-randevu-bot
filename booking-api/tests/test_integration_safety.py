import json
from pathlib import Path

from app import main


class FakeBackgroundTasks:
    def __init__(self):
        self.tasks = []

    def add_task(self, func, *args, **kwargs):
        self.tasks.append((func, args, kwargs))


def test_inbound_dedupe_key_uses_platform_sender_and_message_id():
    private_raw = {"message_id": "mid-1", "message_source": "private_api"}
    graph_raw = {"object": "instagram", "entry": [{"messaging": []}], "message": {"mid": "mid-1"}}

    private_platform = main.extract_inbound_platform(private_raw)
    graph_platform = main.extract_inbound_platform(graph_raw)

    assert private_platform == "instagram_private_api"
    assert graph_platform == "instagram_graph"
    assert main.build_inbound_dedupe_key(private_platform, "user-1", "mid-1") == "instagram_private_api:user-1:mid-1"
    assert main.build_inbound_dedupe_key(graph_platform, "user-1", "mid-1") == "instagram_graph:user-1:mid-1"


def test_has_processed_inbound_message_queries_dedupe_key():
    class Cursor:
        def __init__(self):
            self.sql = ""
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchone(self):
            return {"exists": 1}

    class Conn:
        def __init__(self):
            self.cursor_obj = Cursor()

        def cursor(self):
            return self.cursor_obj

    conn = Conn()

    assert main.has_processed_inbound_message(conn, "instagram_private_api", "user-1", "mid-1") is True
    assert "raw_payload->>'dedupe_key'" in conn.cursor_obj.sql
    assert conn.cursor_obj.params == ("instagram_private_api:user-1:mid-1",)


def test_queue_crm_sync_creates_outbox_event_before_background_task(monkeypatch):
    conversation = {
        "instagram_user_id": "user-1",
        "full_name": "Berkay Fidan",
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "memory_state": {},
    }
    background_tasks = FakeBackgroundTasks()
    calls = []

    monkeypatch.setattr(main, "CRM_SYNC_ENABLED", True)
    monkeypatch.setattr(main, "should_sync_crm_conversation", lambda conversation: True)
    monkeypatch.setattr(
        main,
        "enqueue_crm_sync_outbox",
        lambda conversation, appointment_id, request_metrics: calls.append(
            (conversation, appointment_id, request_metrics)
        )
        or 42,
    )

    queued = main.queue_crm_sync(background_tasks, conversation, 7, {"total_ms": 12})

    assert queued is True
    assert calls and calls[0][1] == 7
    assert background_tasks.tasks
    assert background_tasks.tasks[0][0] == main.sync_crm_outbox_event_safe
    assert background_tasks.tasks[0][1] == (42,)


def test_queue_crm_sync_marks_pending_when_background_sync_fails(monkeypatch):
    events = []

    def fake_sync(event_id):
        events.append(event_id)
        return False

    monkeypatch.setattr(main, "process_crm_sync_outbox_event", fake_sync)

    main.sync_crm_outbox_event_safe(101)

    assert events == [101]


def test_n8n_error_handler_returns_safe_non_technical_fallback():
    workflow_path = Path(__file__).resolve().parents[2] / "workflows" / "instagram-message-bot.json"
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))
    handle_error = next(node for node in workflow["nodes"] if node["name"] == "Handle Error")
    code = handle_error["parameters"]["jsCode"]

    assert "Mesajınızı aldık, kontrol edip size en kısa sürede dönüş yapacağız." in code
    assert "should_reply: true" in code
    assert "reply_text: null" not in code
    assert "Backend processing failed" not in code


def test_poller_uses_safe_fallback_when_processing_chain_fails():
    poller_path = Path(__file__).resolve().parents[2] / "instagram-poller" / "app" / "main.py"
    source = poller_path.read_text(encoding="utf-8")

    assert "SAFE_PROCESSING_FALLBACK_REPLY" in source
    assert "Mesajınızı aldık, kontrol edip size en kısa sürede dönüş yapacağız." in source
    assert '"should_reply": True' in source
    assert '"conversation_state": "processing_failed"' in source
