from fastapi.testclient import TestClient
from app.main import app
import os

client = TestClient(app)
sender_id = "test_500_debug_fast"

messages = [
    "Merhaba, hemen bir ön görüşme ayarlamak istiyorum.",
    "Performans pazarlama",
    "0532 999 88 77",
    "Mehmet Yılmaz",
    "Yarın saat 15:00"
]

print("Starting FastAPI test...")
for msg in messages:
    print(f"\n[USER] {msg}")
    payload = {
        "sender_id": sender_id,
        "message_text": msg
    }
    response = client.post("/api/process-instagram-message", json=payload)
    if response.status_code == 200:
        print(f"[BOT] {response.json().get('reply_text')}")
    else:
        print(f"[ERROR {response.status_code}]")
        print(response.text)
