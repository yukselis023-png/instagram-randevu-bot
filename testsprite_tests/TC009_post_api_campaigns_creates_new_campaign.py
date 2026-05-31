import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_post_api_campaigns_creates_new_campaign():
    url = f"{BASE_URL}/api/campaigns"
    headers = {
        "Content-Type": "application/json"
    }
    # Crafting a valid CampaignCreateRequest payload with synthetic test data
    payload = {
        "name": "Test Campaign - Synthetic",
        "description": "This is a synthetic test campaign created by automated test TC009.",
        "start_date": "2026-06-01T10:00:00Z",
        "end_date": "2026-06-30T18:00:00Z",
        "active": True,
        "target_segments": ["test_segment"],
        "budget": 1000
    }

    response = None
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
        data = response.json()
        # Validate the response contains the created campaign object and fields
        assert isinstance(data, dict), "Response is not a JSON object"
        assert "name" in data and data["name"] == payload["name"], "Campaign name mismatch"
        assert "description" in data and data["description"] == payload["description"], "Campaign description mismatch"
        assert "start_date" in data, "Missing start_date in response"
        assert "end_date" in data, "Missing end_date in response"
        assert "active" in data and data["active"] is True, "Campaign active status mismatch"
        assert "id" in data and isinstance(data["id"], (int, str)), "Campaign ID missing or invalid"
    except (requests.RequestException, AssertionError) as e:
        assert False, f"Test failed with error: {e}"

test_post_api_campaigns_creates_new_campaign()