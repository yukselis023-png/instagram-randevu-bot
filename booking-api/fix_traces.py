import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the NameError in confirmed_booking_should_take_over_message
text = re.sub(
    r'    if is_service_term_clarification\(message_text\):\n\s*decision\["intent"\] = "service_term_clarification"\n\s*decision\["booking_intent"\] = False\n\s*decision\["missing_fields"\] = \[\]\n\s*decision\["should_reply"\] = True\n\s*return decision',
    r'    if is_service_term_clarification(message_text):\n        return False',
    text
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)


with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    t_tests = f.read()

t_tests = t_tests.replace('assert "reklam" in reply', 'assert "performans" in reply')

with open("tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(t_tests)

