from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dm_quality_assertions import assert_no_repeated_replies, assert_quality_reply


def request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: int = 45,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8", errors="replace")
        return response.status, json.loads(raw or "{}")


def post_message(base_url: str, sender: str, username: str, message: str, index: int, timeout: int) -> dict[str, Any]:
    payload = {
        "sender_id": sender,
        "instagram_user_id": sender,
        "instagram_username": username,
        "message_text": message,
        "raw_event": {"message_id": f"{sender}-{index}", "source": "live_smoke_dm_flow"},
        "trace_id": f"{sender}-{index}",
    }
    _, result = request_json("POST", f"{base_url}/api/process-instagram-message", payload=payload, timeout=timeout)
    return result


def cancel_test_appointment(base_url: str, appointment_id: int, timeout: int) -> dict[str, Any]:
    payload = {"status": "cancelled", "note": "live_smoke_dm_flow testi sonrası otomatik iptal"}
    _, result = request_json("PATCH", f"{base_url}/api/appointments/{appointment_id}", payload=payload, timeout=timeout)
    return result


def check_public_endpoints(base_url: str, timeout: int) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for path in ["/health", "/version", "/api/customers", "/api/appointments"]:
        status, body = request_json("GET", f"{base_url}{path}", timeout=timeout)
        if status != 200:
            raise AssertionError(f"{path} returned {status}")
        checks[path] = {"status": status, "keys": sorted(body)[:8]}
    return checks


def run_smoke(base_url: str, *, prefix: str, timeout: int, pause: float) -> dict[str, Any]:
    sender = f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    username = sender.replace("-", "_")
    messages = [
        {"text": "Web sitesi actirmak istiyom", "expected_any": ["web", "site"]},
        {"text": "Tamam", "expected_any": ["on gorusme", "ön görüşme", "ad", "soyad"]},
        {"text": "Berkay Elbir", "expected_any": ["telefon", "numara"]},
        {"text": "05555555555", "expected_any": ["uygun", "secenek", "seçenek", "saat"]},
    ]
    results: list[dict[str, Any]] = []
    appointment_ids: list[int] = []

    for index, step in enumerate(messages, start=1):
        message = step["text"]
        result = post_message(base_url, sender, username, message, index, timeout)
        reply = result.get("reply_text") or ""
        metrics = result.get("metrics") or {}
        if metrics.get("reply_engine") != "ai_first_v5":
            raise AssertionError(f"unexpected reply_engine at step {index}: {metrics.get('reply_engine')!r}")
        compact = {
            "message": message,
            "should_reply": result.get("should_reply"),
            "reply_text": reply,
            "metrics": {
                "reply_engine": metrics.get("reply_engine"),
                "workflow_action": metrics.get("workflow_action"),
                "booking_stage": metrics.get("booking_stage"),
                "slot_resolution": metrics.get("slot_resolution"),
            },
            "appointment_created": result.get("appointment_created"),
            "appointment_id": result.get("appointment_id"),
        }
        assert_quality_reply(
            f"live smoke step {index}",
            message,
            compact,
            expected_any=step["expected_any"],
            forbidden=["anlasilmadi", "lutfen daha acik", "needed bilgi"],
        )
        if result.get("appointment_created") and result.get("appointment_id"):
            appointment_ids.append(int(result["appointment_id"]))
        results.append(compact)
        time.sleep(pause)

    slot_reply = results[-1]["reply_text"]
    slot_match = re.search(r"(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})", slot_reply)
    if not slot_match:
        raise AssertionError(f"live smoke did not receive slot suggestions: {slot_reply!r}")

    selected_slot = f"{slot_match.group(1)} {slot_match.group(2)}"
    result = post_message(base_url, sender, username, selected_slot, len(messages) + 1, timeout)
    metrics = result.get("metrics") or {}
    if metrics.get("reply_engine") != "ai_first_v5":
        raise AssertionError(f"unexpected reply_engine at slot selection: {metrics.get('reply_engine')!r}")
    compact = {
        "message": selected_slot,
        "should_reply": result.get("should_reply"),
        "reply_text": result.get("reply_text") or "",
        "metrics": {
            "reply_engine": metrics.get("reply_engine"),
            "workflow_action": metrics.get("workflow_action"),
            "booking_stage": metrics.get("booking_stage"),
            "slot_resolution": metrics.get("slot_resolution"),
        },
        "appointment_created": result.get("appointment_created"),
        "appointment_id": result.get("appointment_id"),
    }
    assert_quality_reply(
        "live smoke slot selection",
        selected_slot,
        compact,
        expected_any=["randevu", "kayit", "oluşturuldu", "olusturuldu"],
        forbidden=["anlasilmadi", "lutfen daha acik", "needed bilgi"],
    )
    if result.get("appointment_created") and result.get("appointment_id"):
        appointment_ids.append(int(result["appointment_id"]))
    results.append(compact)

    assert_no_repeated_replies(results)

    cancellations = []
    for appointment_id in appointment_ids:
        cancellations.append(cancel_test_appointment(base_url, appointment_id, timeout))

    if not appointment_ids:
        raise AssertionError("live smoke did not create an appointment")

    return {
        "sender": sender,
        "endpoint_checks": check_public_endpoints(base_url, timeout),
        "results": results,
        "cancelled_appointment_ids": appointment_ids,
        "cancellations": cancellations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DOEL Instagram bot live smoke DM flow against a backend API.")
    parser.add_argument("--base-url", default="https://instagram-randevu-bot.onrender.com", help="Backend base URL")
    parser.add_argument("--prefix", default="smoke-dm-flow", help="Test instagram_user_id prefix")
    parser.add_argument("--timeout", type=int, default=45, help="HTTP timeout seconds")
    parser.add_argument("--pause", type=float, default=1.0, help="Pause between messages")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    try:
        summary = run_smoke(base_url, prefix=args.prefix, timeout=args.timeout, pause=args.pause)
    except (AssertionError, urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps({"ok": True, **summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
