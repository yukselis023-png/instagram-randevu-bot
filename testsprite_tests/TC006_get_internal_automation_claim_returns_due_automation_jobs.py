import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30


def test_get_internal_automation_claim_returns_due_automation_jobs():
    url = f"{BASE_URL}/internal/automation/claim"
    headers = {
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
    except requests.RequestException as e:
        assert False, f"Request to {url} failed: {e}"

    assert response.status_code == 200, f"Expected status code 200 but got {response.status_code}"

    try:
        json_data = response.json()
    except ValueError:
        assert False, "Response is not valid JSON"

    assert isinstance(json_data, dict), "Response JSON should be an object"


test_get_internal_automation_claim_returns_due_automation_jobs()
