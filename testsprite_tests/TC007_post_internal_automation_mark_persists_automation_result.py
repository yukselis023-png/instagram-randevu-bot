import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30


def test_post_internal_automation_mark_persists_automation_result():
    url = f"{BASE_URL}/internal/automation/mark"
    headers = {
        "Content-Type": "application/json"
    }
    # Based on the PRD, the request body is expected to be according to AutomationMarkRequest,
    # but since no detailed schema is given, we create a synthetic success result payload to test persisting automation result.
    # Using a plausible example for success marking.
    payload = {
        "job_id": "test_job_123",
        "status": "success",
        "details": {
            "message": "Automation job completed successfully.",
            "timestamp": "2026-05-20T12:00:00Z"
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    except requests.RequestException as e:
        assert False, f"Request failed: {e}"

    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    # Response is expected to be 200 confirming persistence; no detailed schema is specified.
    # Check that response is JSON object
    try:
        resp_json = response.json()
    except ValueError:
        assert False, "Response is not valid JSON"

    # Optionally validate response content if any key expected
    assert isinstance(resp_json, dict), "Response JSON is not an object"


test_post_internal_automation_mark_persists_automation_result()