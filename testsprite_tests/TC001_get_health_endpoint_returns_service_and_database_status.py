import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_get_health_endpoint_returns_service_and_database_status():
    url = f"{BASE_URL}/health"
    try:
        response = requests.get(url, timeout=TIMEOUT)
    except requests.RequestException as e:
        assert False, f"Request to /health endpoint failed: {e}"

    assert response.status_code == 200, f"Expected HTTP 200, got {response.status_code}"

    try:
        data = response.json()
    except ValueError:
        assert False, "Response is not valid JSON"

    # Basic checks for expected keys that typically appear in health status
    # Since the exact schema is not provided, we check for some common keys that would hold service and db/runtime status.
    # Adjust keys if schema is known.
    assert isinstance(data, dict), "Response JSON should be an object"

    service_keys = ["service", "status", "database", "runtime", "uptime"]
    # At least one of these keys should be present to consider health status info meaningful.
    assert any(key in data for key in service_keys), "Health status object does not contain expected status keys"

test_get_health_endpoint_returns_service_and_database_status()