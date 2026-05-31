import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'booking-api')))

# Mock things so dateparser issue doesn't crash the import
import sys
from unittest.mock import MagicMock
sys.modules['dateparser'] = MagicMock()
sys.modules['dateparser.search'] = MagicMock()

from app.main import update_conversation_memory_from_user_message, update_conversation_memory_after_bot_reply, ensure_conversation_memory

conv = {
    "state": "info",
    "memory_summary": {"offer_status": "declined", "open_loop": "decline_cooldown", "pending_offer": None}
}

update_conversation_memory_from_user_message("Pekala", conv, [], {"did_user_accept_previous_offer": "false"})
print(f"After User 'Pekala': {conv['memory_summary']}")

update_conversation_memory_after_bot_reply(conv, "Rica ederiz.", decision_label="info:decline_cooldown")
print(f"After Bot 'info:decline_cooldown': {conv['memory_summary']}")
