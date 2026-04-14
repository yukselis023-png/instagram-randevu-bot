import json
import sys
import time
import urllib.request
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "http://localhost:18000/api/process-instagram-message"
SCENARIOS = [
    ("bench-greeting", "Merhaba"),
    ("bench-price", "Web tasarım fiyat bilgisi alabilir miyim?"),
    ("bench-advice", "Instagramdan reklam da yapıyor musunuz, bana ne uygun olur?"),
    ("bench-compare", "Reklam mı otomasyon mu bana daha uygun olur?"),
    ("bench-availability", "Yarın 14:00 uygun mu acaba, numaram 05301234567"),
]


def call_api(sender_id: str, text: str) -> dict:
    payload = {
        "sender_id": sender_id,
        "instagram_username": sender_id,
        "message_text": text,
        "raw_event": {"message_id": f"{sender_id}-{int(time.time() * 1000)}"},
    }
    req = urllib.request.Request(
        BASE_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=90) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    wall_ms = round((time.perf_counter() - started) * 1000)
    return {
        "sender_id": sender_id,
        "text": text,
        "wall_ms": wall_ms,
        "metrics": body.get("metrics") or {},
        "decision_path": body.get("decision_path") or [],
        "conversation_state": body.get("conversation_state"),
        "reply_text": body.get("reply_text"),
    }


if __name__ == "__main__":
    results = [call_api(sender, text) for sender, text in SCENARIOS]
    json.dump(
        {"generated_at": datetime.utcnow().isoformat() + "Z", "results": results},
        sys.stdout,
        ensure_ascii=False,
        indent=2,
    )
    sys.stdout.write("\n")
