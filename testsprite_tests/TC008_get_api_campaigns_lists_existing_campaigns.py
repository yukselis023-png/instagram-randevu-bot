import requests
import uuid

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_get_api_campaigns_lists_existing_campaigns():
    # Create a new campaign to ensure it will appear in the list
    campaign_create_url = f"{BASE_URL}/api/campaigns"
    unique_suffix = str(uuid.uuid4())
    campaign_payload = {
        "name": f"Test Campaign {unique_suffix}",
        "description": "Campaign created for automated test TC008",
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "budget": 1000,
        "channel": "instagram",
        "active": True
    }

    # POST a new campaign
    created_campaign = None
    try:
        resp_create = requests.post(campaign_create_url, json=campaign_payload, timeout=TIMEOUT)
        assert resp_create.status_code == 200, f"Expected 200 OK on campaign creation but got {resp_create.status_code}"
        created_campaign = resp_create.json()
        assert "id" in created_campaign or "campaign_id" in created_campaign, "Created campaign response missing identifier"

        # GET the list of campaigns
        resp_list = requests.get(campaign_create_url, timeout=TIMEOUT)
        assert resp_list.status_code == 200, f"Expected 200 OK on campaign list but got {resp_list.status_code}"
        campaigns = resp_list.json()

        # We expect the campaigns response to be a container (list or dict) including the newly created campaign
        # Handle the main container if dict with data key or list directly
        if isinstance(campaigns, dict):
            # If response contains list under some key, try common keys
            if "campaigns" in campaigns and isinstance(campaigns["campaigns"], list):
                campaigns_list = campaigns["campaigns"]
            elif "items" in campaigns and isinstance(campaigns["items"], list):
                campaigns_list = campaigns["items"]
            else:
                campaigns_list = list(campaigns.values())
        elif isinstance(campaigns, list):
            campaigns_list = campaigns
        else:
            campaigns_list = []

        # Assert the created campaign is in the list by unique name
        campaign_names = [c.get("name", "") for c in campaigns_list]
        assert campaign_payload["name"] in campaign_names, "Created campaign not found in campaign list"

    finally:
        # Do not delete production data as per instructions. Skip cleanup.

        pass

test_get_api_campaigns_lists_existing_campaigns()