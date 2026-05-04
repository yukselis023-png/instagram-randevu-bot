with open("tests/test_generic_business_configs.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace trailing spaces
import re
text = re.sub(r'conv\["state"\] = "collect_name"\n\s+conv\["service"\] = "İmplant"', 'conv["state"] = "collect_name"\n        conv["service"] = "İmplant"', text)

with open("tests/test_generic_business_configs.py", "w", encoding="utf-8") as f:
    f.write(text)
