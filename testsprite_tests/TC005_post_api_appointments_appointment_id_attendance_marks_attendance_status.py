import requests
import uuid

BASE_URL = "http://localhost:18000"
TIMEOUT = 30

def test_post_api_appointments_appointment_id_attendance_marks_attendance_status():
    headers = {"Content-Type": "application/json"}

    # Step 1: Create a synthetic appointment via Instagram DM processing (to get an appointment_id)
    # Compose a synthetic Instagram message that results in an appointment being created
    sender_id = f"testuser_{uuid.uuid4()}"
    insta_msg_payload = {
        "sender_id": sender_id,
        "message_text": "I want to book an appointment for testing attendance",
        "raw_event": {"event_id": str(uuid.uuid4()), "type": "message"}
    }

    create_appointment_resp = requests.post(
        f"{BASE_URL}/api/process-instagram-message",
        json=insta_msg_payload,
        headers=headers,
        timeout=TIMEOUT
    )
    assert create_appointment_resp.status_code == 200, f"Failed to create appointment via Instagram message: {create_appointment_resp.text}"
    create_resp_json = create_appointment_resp.json()

    # Ensure the response indicates an appointment was created, we try to find the appointment_id
    # The PRD does not detail exact schema, so we fetch appointments for this user next.

    # Step 2: Fetch appointments to find the appointment_id for this test user
    params = {"filters": f"customer_instagram_id=={sender_id}"}
    get_appointments_resp = requests.get(
        f"{BASE_URL}/api/appointments",
        params=params,
        headers=headers,
        timeout=TIMEOUT
    )
    assert get_appointments_resp.status_code == 200, f"Failed to list appointments: {get_appointments_resp.text}"
    appointments = get_appointments_resp.json().get("items", [])
    # Find one that belongs to this sender_id
    appointment_id = None
    for appt in appointments:
        # Some appointment structure is expected to have customer id/link or the sender_id itself
        # We'll trust filtering by filter worked and pick the first
        appointment_id = appt.get("id") or appt.get("appointment_id")
        if appointment_id:
            break
    assert appointment_id, "No appointment found for the test user"

    try:
        # Step 3: POST attendance with status "attended"
        attendance_payload = {"attendance_status": "attended"}
        post_attendance_resp = requests.post(
            f"{BASE_URL}/api/appointments/{appointment_id}/attendance",
            json=attendance_payload,
            headers=headers,
            timeout=TIMEOUT
        )
        assert post_attendance_resp.status_code == 200, f"POST attendance failed: {post_attendance_resp.text}"
        attendance_resp_json = post_attendance_resp.json()
        # We expect response to confirm attendance marked; check for a confirmation or status field
        assert (
            "attendance_status" in attendance_resp_json and attendance_resp_json["attendance_status"] == "attended"
        ) or ("message" in attendance_resp_json), "Attendance status confirmation missing or incorrect"

        # Step 4: GET the appointments again to verify updated attendance
        get_appointments_post_resp = requests.get(
            f"{BASE_URL}/api/appointments",
            params=params,
            headers=headers,
            timeout=TIMEOUT
        )
        assert get_appointments_post_resp.status_code == 200, f"Failed to get appointments after attendance update: {get_appointments_post_resp.text}"
        appointments_after = get_appointments_post_resp.json().get("items", [])
        updated_appt = None
        for appt in appointments_after:
            appt_id = appt.get("id") or appt.get("appointment_id")
            if appt_id == appointment_id:
                updated_appt = appt
                break
        assert updated_appt is not None, "Appointment missing after attendance update"
        # Check attendance status is updated accordingly
        assert (
            updated_appt.get("attendance_status") == "attended" or
            updated_appt.get("attendance") == "attended"
        ), "Attendance status not updated to 'attended' in appointment record"

    finally:
        # Cleanup: Do NOT delete production data per instructions, so skip deletion.
        pass


test_post_api_appointments_appointment_id_attendance_marks_attendance_status()