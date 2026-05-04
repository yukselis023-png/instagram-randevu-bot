with open("comprehensive_live_test.py","r",encoding="utf-8") as f:
    code = f.read()
import re
code = re.sub(r'payload = \{.*?\}', 'payload = {"sender_id": sender_id, "message_text": msg}', code, flags=re.DOTALL)
with open("comprehensive_live_test.py","w",encoding="utf-8") as f:
    f.write(code)
