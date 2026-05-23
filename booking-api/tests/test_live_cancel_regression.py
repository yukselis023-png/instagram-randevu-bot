from datetime import date, timedelta
from unittest.mock import patch

from fastapi import BackgroundTasks
from fastapi.testclient import TestClient

from app.main import IncomingMessage, app, process_instagram_message


def _raw(mid: str):
    return {"platform": "test", "message_id": mid, "event_id": mid}


def test_live_cancel_request_updates_appointment_status():
    client = TestClient(app)
    sender = "cancel-live-regression-001"
    target_date = date.today() + timedelta(days=7)
    cancel_msg = "Vazgeçtim, bu randevuyu iptal eder misiniz?"

    create_payload = IncomingMessage(
        sender_id=sender,
        instagram_username="cancel_regression",
        message_text=f"Adım Cancel Regression, telefonum 0555 111 22 33. Web Tasarım için {target_date.strftime('%d.%m.%Y')} saat 13:00 ön görüşme almak istiyorum.",
        raw_event=_raw("create-cancel-regression"),
    )
    created = process_instagram_message(create_payload, BackgroundTasks())
    assert created.appointment_created is True

    cancel_payload = IncomingMessage(
        sender_id=sender,
        instagram_username="cancel_regression",
        message_text=cancel_msg,
        raw_event=_raw("cancel-cancel-regression"),
    )
    cancelled = process_instagram_message(cancel_payload, BackgroundTasks())
    assert "iptal" in cancelled.reply_text.lower()

    response = client.get(f"/api/appointments?date_from={target_date.isoformat()}&date_to={target_date.isoformat()}&limit=1000")
    assert response.status_code == 200
    rows = response.json()["appointments"]
    row = next(item for item in rows if item["instagram_user_id"] == sender)
    assert row["status"] == "cancelled"
    assert row["attendance_status"] == "cancelled"
