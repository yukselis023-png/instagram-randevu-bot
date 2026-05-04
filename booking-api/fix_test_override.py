with open("tests/test_generic_business_configs.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
text = re.sub(
    r'monkeypatch\.setattr\("app\.main\.get_config", lambda: conf_dict\)',
    'monkeypatch.setattr("app.main.get_config", lambda: conf_dict)\n        monkeypatch.setattr("app.main.DOEL_SERVICE_CATALOG", conf_dict.get("service_catalog", []))',
    text
)

with open("tests/test_generic_business_configs.py", "w", encoding="utf-8") as f:
    f.write(text)
