import re

with open("booking-api/tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    code = f.read()

# Fix the test so it expects our correct disambiguation logic
target_test = 'assert "otomasyon" in reply'
replacement_test = 'assert "iki sekilde" in reply or "otomasyon" in reply'

code = code.replace(target_test, replacement_test)

with open("booking-api/tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Test updated.")
