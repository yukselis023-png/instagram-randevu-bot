from app.generic_core import handle_generic_chatbot_message
import os
os.environ["LLM_MODEL"] = "llama-3.3-70b-versatile"
sender_id = "test_500_debug"

messages = [
    "Merhaba, hemen bir ön görüşme ayarlamak istiyorum.",
    "Performans pazarlama hizmetinizle ilgileniyorum.",
    "Telefonum 0532 999 88 77",
    "İsmim Mehmet Yılmaz",
    "Yarın öğleden sonra saat 15:00 toplantı için uygun mu?"
]

print("Starting debug...")
for msg in messages:
    print(f"\n[USER] {msg}")
    reply = handle_generic_chatbot_message(sender_id, msg, "doel")
    print(f"[BOT] {reply}")
