import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'booking-api')))
from app.main import update_conversation_memory_from_user_message, is_confirmation_acceptance_message, match_objection_type, llm_bool

conversation = {
    "state": "info",
    "memory_summary": {"offer_status": "declined", "open_loop": "decline_cooldown", "pending_offer": None}
}
print(f"Before: {conversation['memory_summary']}")
update_conversation_memory_from_user_message("Pekala", conversation, [], {"did_user_accept_previous_offer": "false"})
print(f"After (No LLM accept): {conversation['memory_summary']}")

conversation = {
    "state": "info",
    "memory_summary": {"offer_status": "declined", "open_loop": "decline_cooldown", "pending_offer": None}
}
update_conversation_memory_from_user_message("Pekala", conversation, [], {"did_user_accept_previous_offer": "true"})
print(f"After (LLM accept=true): {conversation['memory_summary']}")

print("is_confirmation_acceptance_message('Pekala'):", is_confirmation_acceptance_message("Pekala"))
print("match_objection_type('Pekala'):", match_objection_type("Pekala"))
