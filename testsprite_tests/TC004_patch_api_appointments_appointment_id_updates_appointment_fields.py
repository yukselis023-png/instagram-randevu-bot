import requests

BASE_URL = "http://localhost:18000"
TIMEOUT = 30


def test_patch_api_appointments_appointment_id_updates_appointment_fields():
    appointment_id = None
    headers = {"Content-Type": "application/json"}

    # Step 1: Get an existing appointment to update
    try:
        response = requests.get(f"{BASE_URL}/api/appointments", timeout=TIMEOUT)
        assert response.status_code == 200, f"Unexpected status code on listing appointments: {response.status_code}"
        appointments = response.json()
        assert isinstance(appointments, list) or isinstance(appointments, dict), "Appointments response is not list or dict"

        # Try to find an appointment ID
        # appointments could be list or object containing data; try common keys
        if isinstance(appointments, dict):
            # If dict, try keys 'items', 'data', else treat as list under itself
            if "items" in appointments and isinstance(appointments["items"], list):
                appointments_list = appointments["items"]
            elif "data" in appointments and isinstance(appointments["data"], list):
                appointments_list = appointments["data"]
            else:
                # Maybe just dict converted from list
                appointments_list = list(appointments.values())
        else:
            appointments_list = appointments

        for appt in appointments_list:
            if isinstance(appt, dict) and "id" in appt:
                appointment_id = appt["id"]
                break

        if appointment_id is None:
            raise Exception("No available appointment to update found")

        # Step 2: Test valid PATCH update
        update_payload = {
            "notes": "Updated by automated test",
            "status": "confirmed"
        }

        patch_resp = requests.patch(
            f"{BASE_URL}/api/appointments/{appointment_id}",
            json=update_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        assert patch_resp.status_code == 200, f"Valid PATCH request failed with status {patch_resp.status_code}"
        patch_data = patch_resp.json()
        # Basic confirmation: check at least returned data includes updated fields if any
        # (API schema not fully specific, so we just check it returns dict)
        assert isinstance(patch_data, dict), "PATCH response JSON is not an object"

        # Step 3: Test PATCH with invalid appointment_id
        invalid_id = "non-existent-id-1234567890"
        invalid_resp = requests.patch(
            f"{BASE_URL}/api/appointments/{invalid_id}",
            json=update_payload,
            headers=headers,
            timeout=TIMEOUT,
        )
        assert invalid_resp.status_code != 200, "PATCH with invalid appointment_id unexpectedly succeeded"
        # Expect 404 or 400 or 422, accept any error status code >=400
        assert invalid_resp.status_code >= 400

        # Step 4: Test PATCH with invalid body (e.g. wrong field types or empty body)
        invalid_body = {"status": 12345}  # assuming status should be string, not int
        invalid_body_resp = requests.patch(
            f"{BASE_URL}/api/appointments/{appointment_id}",
            json=invalid_body,
            headers=headers,
            timeout=TIMEOUT,
        )
        assert invalid_body_resp.status_code != 200, "PATCH with invalid body unexpectedly succeeded"
        assert invalid_body_resp.status_code >= 400

    except Exception as e:
        raise
    # Do not delete production data, so no cleanup.


test_patch_api_appointments_appointment_id_updates_appointment_fields()