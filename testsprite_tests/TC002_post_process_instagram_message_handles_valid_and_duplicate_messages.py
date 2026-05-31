import requests
import uuid

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_post_process_instagram_message_handles_valid_and_duplicate_messages():
    url = f"{BASE_URL}/api/process-instagram-message"
    # Create a synthetic unique sender_id for this test
    sender_id = f"test_sender_{uuid.uuid4()}"
    message_text = "Test appointment booking message"
    raw_event = {
        "id": str(uuid.uuid4()),
        "timestamp": 1234567890,
        "type": "message",
        "platform": "instagram"
    }
    payload = {
        "sender_id": sender_id,
        "message_text": message_text,
        "raw_event": raw_event
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        # First POST request with a valid payload
        response_first = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        assert response_first.status_code == 200, f"Expected 200 OK, got {response_first.status_code}"
        json_first = response_first.json()
        # Validate should_reply is True and bot_reply text exists
        assert "should_reply" in json_first, "'should_reply' field missing in response"
        assert json_first["should_reply"] is True, "Expected should_reply to be True on first request"
        assert "bot_reply" in json_first or "reply_text" in json_first, "Expected bot reply in response"

        # Second POST request with the identical payload (same sender_id and raw_event)
        response_second = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        assert response_second.status_code == 200, f"Expected 200 OK, got {response_second.status_code}"
        json_second = response_second.json()
        # Validate duplicate indication and no new reply text
        assert "duplicate" in json_second, "'duplicate' field missing in duplicate response"
        assert json_second["duplicate"] is True, "Expected duplicate to be True on second identical request"
        # should_reply should be false in duplicate response
        assert "should_reply" in json_second, "'should_reply' field missing in duplicate response"
        assert json_second["should_reply"] is False, "Expected should_reply to be False on duplicate request"
        # bot_reply or reply_text should be empty or not present
        bot_reply_second = json_second.get("bot_reply") or json_second.get("reply_text")
        assert not bot_reply_second, "Expected no bot reply on duplicate request"

    except (requests.RequestException, AssertionError) as e:
        raise AssertionError(f"Test failed due to error: {e}")

test_post_process_instagram_message_handles_valid_and_duplicate_messages()