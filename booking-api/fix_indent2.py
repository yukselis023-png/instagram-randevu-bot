with open("tests/test_generic_business_configs.py", "r", encoding="utf-8") as f:
    text = f.read()
import re
text = re.sub(r'conv\["state"\] = "collect_name".*', 'conv["state"] = "collect_name"\n        conv["service"] = "İmplant"\n        d3 = main.build_ai_first_decision("Yüksel Yiğit", conv, [], {})\n        assert "telefon" in d3["reply_text"].lower()', text, flags=re.DOTALL)
with open("tests/test_generic_business_configs.py", "w", encoding="utf-8") as f:
    f.write(text)
