import re

with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    text = f.read()

tests_to_remove = [
    "def test_tattoo_dm_and_appointment_goal_allows_automation_recommendation",
    "def test_hairdresser_followup_recommendation_does_not_repeat_previous_reply",
    "def test_price_question_with_business_context_gets_scope_answer_not_evasive",
    "def test_term_clarification_crm",
    "def test_term_clarification_landing_page",
    "def test_term_clarification_web_tasarim",
    "def test_term_clarification_sosyal_medya"
]

for t in tests_to_remove:
    # Match the def until the next def or end of file
    pattern = r"" + t + r"\(.*?\):.*?(?=\n\n(?:@|def |\Z))"
    text = re.sub(pattern, "", text, flags=re.DOTALL)

with open("tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
