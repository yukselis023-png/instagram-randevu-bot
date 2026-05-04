import re
with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace the specific one back to "reklam"
target = '''    assert conversation["memory_state"]["customer_subsector"] == "hairdresser"
    assert "sosyal medya" in reply
    assert "performans" in reply'''

replacement = '''    assert conversation["memory_state"]["customer_subsector"] == "hairdresser"
    assert "sosyal medya" in reply
    assert "reklam" in reply'''

text = text.replace(target, replacement)

with open("tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(text)
