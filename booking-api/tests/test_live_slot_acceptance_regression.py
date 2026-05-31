import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from test_automation_invariants import run_message  # noqa: E402
import app.generic_core as gc


def test_suggested_slot_selection_creates_appointment_and_updates_state(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "slot-selection-regression",
        "state": "collect_datetime",
        "full_name": "Canli Test Kullanicisi",
        "lead_name": "Canli Test Kullanicisi",
        "phone": "+905552223344",
        "service": "Web Tasarim",
        "requested_date": "2026-05-21",
        "requested_time": None,
        "appointment_status": "collecting",
        "memory_state": {
            "requested_service": "Web Tasarim",
            "selected_service": "Web Tasarim",
            "contact_channel": "instagram_dm",
            "suggested_booking_slots": [
                {"date": "2026-05-21", "time": "10:00"},
                {"date": "2026-05-21", "time": "11:00"},
                {"date": "2026-05-21", "time": "12:00"},
            ],
        },
    }
    created_payload = {}

    def fake_conflict(*args, **kwargs):
        return None

    def fake_validate(*args, **kwargs):
        return None

    def fake_create(conn, conv, username=None):
        created_payload.update(conv)
        return 987

    monkeypatch.setattr(gc, "find_existing_appointment", fake_conflict)
    monkeypatch.setattr(gc, "validate_slot", fake_validate)
    monkeypatch.setattr(gc, "create_appointment", fake_create)

    result = run_message(
        monkeypatch,
        "11:00 uygun, lütfen kaydı oluşturun.",
        {
            "intent": "active_booking",
            "reply_text": "Yarın saat 11:00 için ön görüşmenizi not aldım.",
            "extracted_entities": {"requested_time": "11:00"},
            "requires_human": False,
        },
        conversation,
        config={
            "business_name": "DOEL Digital",
            "service_catalog": [
                {"display": "Web Tasarim", "name": "Web Tasarim", "keywords": ["web", "web tasarim", "site"]}
            ],
            "human_contact_name": "Berkay",
        },
    )

    assert result.appointment_created is True
    assert result.appointment_id == 987
    assert conversation["state"] == "completed"
    assert conversation["appointment_status"] == "confirmed"
    assert conversation["appointment_id"] == 987
    assert conversation["requested_date"] == "2026-05-21"
    assert conversation["requested_time"] == "11:00"
    assert created_payload["requested_time"] == "11:00"
