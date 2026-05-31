import requests
import uuid

BASE_URL = "http://localhost:18000"
TIMEOUT = 30


def test_get_api_customers_instagram_user_id_returns_customer_detail_or_404():
    # To test the known user case, first create a synthetic local test user via process-instagram-message API
    create_msg_url = f"{BASE_URL}/api/process-instagram-message"
    known_instagram_user_id = f"testuser_{uuid.uuid4().hex[:8]}"
    message_payload = {
        "sender_id": known_instagram_user_id,
        "message_text": "Hello, this is a test message.",
        "raw_event": {"event_type": "test"}
    }

    try:
        # Create a synthetic known user by sending an Instagram message
        create_resp = requests.post(create_msg_url, json=message_payload, timeout=TIMEOUT)
        assert create_resp.status_code == 200, f"Failed to create test user, status: {create_resp.status_code}"

        # Now attempt to get customer detail for the known user - should return 200 with customer details
        get_customer_url = f"{BASE_URL}/api/customers/{known_instagram_user_id}"
        get_resp = requests.get(get_customer_url, timeout=TIMEOUT)
        assert get_resp.status_code == 200, f"Expected 200 for known user, got {get_resp.status_code}"
        json_resp = get_resp.json()
        assert isinstance(json_resp, dict), "Response for known user is not a JSON object"
        assert "instagram_user_id" in json_resp or "id" in json_resp or "customer_id" in json_resp, \
            "Returned customer detail missing expected identifier fields"

        # Test unknown user id returns 404 Not Found
        unknown_user_id = f"unknownuser_{uuid.uuid4().hex[:8]}"
        get_unknown_url = f"{BASE_URL}/api/customers/{unknown_user_id}"
        get_unknown_resp = requests.get(get_unknown_url, timeout=TIMEOUT)
        assert get_unknown_resp.status_code == 404, f"Expected 404 for unknown user, got {get_unknown_resp.status_code}"

    except requests.RequestException as e:
        assert False, f"Request failed: {e}"


test_get_api_customers_instagram_user_id_returns_customer_detail_or_404()