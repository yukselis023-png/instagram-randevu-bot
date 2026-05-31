import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_get_api_llm_health_returns_llm_provider_connectivity_status():
    url = f"{BASE_URL}/api/llm-health"
    headers = {
        "Accept": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        assert False, f"Request to {url} failed: {e}"

    assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    try:
        data = response.json()
    except ValueError:
        assert False, "Response is not valid JSON"

    assert isinstance(data, dict), "Response JSON is not an object"


test_get_api_llm_health_returns_llm_provider_connectivity_status()