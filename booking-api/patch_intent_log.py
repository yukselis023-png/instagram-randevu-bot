import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Fix the trigger logic
old_func = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey mi", "aynı şey mi", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing", "web tasarim", "web sitesi", "website", 
                 "sosyal medya", "reklam", "performans"]
        return any(term in lowered for term in terms)
    except:
        return False"""

new_func = """def is_service_term_clarification(text: str) -> bool:
    try:
        from app.main import sanitize_text
        lowered = sanitize_text(text).lower()
        triggers = ["ne demek", "ayni sey", "aynı şey", "neyi kapsiyor", "neyi kapsıyor", 
                    "nasil calisiyor", "nasıl çalışıyor", "farki ne", "farkı ne", "farki nedir", "nedir"]
        if not any(t in lowered for t in triggers):
            return False
            
        terms = ["otomasyon", "crm", "landing", "web", "sosyal medya", "reklam", "performans"]
        return any(term in lowered for term in terms)
    except:
        return False"""

text = text.replace(old_func, new_func)


# 2. Inject Logging to `generate_reply` before Return
logger_code = """
import logging
logger = logging.getLogger(__name__)

def __log_instagram_debug_trace(
    user_message: str,
    memory_before: dict,
    memory_after: dict,
    conversation_id: str,
    sender_id: str,
    channel: str,
    decision: dict,
    reply_text: str
):
    try:
        detect_intent = decision.get("intent", "unknown")
        ai_candidate = decision.get("ai_draft", "N/A")  # we may not have this here but we log what we have
        
        log_block = f\"\"\"
============== INSTAGRAM DM DEBUG TRACE ==============
deploy_commit: 9b96231
source: {channel}
inbound_endpoint: /webhook
sender_id: {sender_id}
conversation_id: {conversation_id}
user_message: {user_message}
normalized_message: {sanitize_text(user_message).lower()}
detected_intent: {detect_intent}
selected_route: direct_answer/{detect_intent}
memory_before: {memory_before}
memory_after: {memory_after}
ai_candidate_reply: {decision.get("original_ai_reply", "None")}
final_reply: {reply_text}
outbound_reply: {reply_text}
outbound_channel: {channel}
======================================================
\"\"\"
        print(log_block)
        logger.warning(log_block)
    except Exception as e:
        print("Log error:", e)

"""

# Let's cleanly put it at the end of the file
# and we need to call it from generate_reply at the end

if "__log_instagram_debug_trace" not in text:
    text += logger_code

# In generate_reply, we find return final_reply, conversation => and inject our log
gen_reply_end = """    return final_reply, conversation"""
gen_reply_injected = """    __log_instagram_debug_trace(
        user_message=message_text,
        memory_before=memory_before_snap,
        memory_after=conversation.get("memory_state", {}),
        conversation_id=conversation.get("id", "none"),
        sender_id=conversation.get("sender_id", "none"),
        channel=conversation.get("channel", "instagram"),
        decision=decision,
        reply_text=final_reply
    )
    return final_reply, conversation"""

# I need to save memory_before somehow
gen_reply_sig = """def generate_reply(message_text: str, conversation: dict[str, Any]) -> tuple[str, dict[str, Any]]:"""
if "memory_before_snap =" not in text:
    text = text.replace(gen_reply_sig, gen_reply_sig + '\n    import copy\n    memory_before_snap = copy.deepcopy(conversation.get("memory_state", {}))')

if gen_reply_end in text:
    text = text.replace(gen_reply_end, gen_reply_injected)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("done")
