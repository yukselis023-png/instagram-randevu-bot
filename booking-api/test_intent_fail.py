import sys
from app.main import is_service_term_clarification, apply_ai_first_quality_overrides

msg = "Web tasarımla web sitesi aynı şey değil mi?"
print("is_service_term_clarification:", is_service_term_clarification(msg))

decision = apply_ai_first_quality_overrides(msg, {}, {"state": "new"}, [])
print("intent:", decision.get("intent"))
print("reply:", decision.get("reply_text"))
