"""Live Doel CRM source-of-truth read layer.

Conversation memory is never authoritative. Use CRM state first, local DB only as fallback.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ACTIVE_STATUSES = {"confirmed", "preconsultation", "scheduled", "active"}
INACTIVE_ATTENDANCE = {"completed", "no_show", "canceled", "cancelled"}


@dataclass
class AppointmentSnapshot:
    id: str
    customer_name: str
    service: str
    date: str
    time: str
    status: str
    source_appointment_id: str | None = None


@dataclass
class CustomerSnapshot:
    id: str
    name: str
    phone: str
    instagram_user_id: str


@dataclass
class CRMState:
    customer: CustomerSnapshot | None
    active_appointments: list[AppointmentSnapshot]
    source: str = "crm"


def _norm(value: Any) -> str:
    return str(value or "").strip()


def extract_crm_state_from_payload(payload: dict[str, Any], instagram_user_id: str) -> CRMState:
    """Extract customer + active appointments from normalized CRM workspace payload."""
    ig = _norm(instagram_user_id)
    customers = payload.get("customers") if isinstance(payload.get("customers"), list) else []
    appointments = payload.get("appointments") if isinstance(payload.get("appointments"), list) else []

    customer_row = next((c for c in customers if _norm(c.get("instagramUserId")) == ig), None)
    customer = None
    if customer_row:
        customer = CustomerSnapshot(
            id=_norm(customer_row.get("id")),
            name=_norm(customer_row.get("authorizedPerson") or customer_row.get("name")),
            phone=_norm(customer_row.get("phone")),
            instagram_user_id=ig,
        )

    active: list[AppointmentSnapshot] = []
    for row in appointments:
        if _norm(row.get("instagramUserId")) != ig:
            continue
        status = _norm(row.get("status")).lower()
        attendance = _norm(row.get("attendanceStatus") or row.get("attendance_status")).lower()
        if status and status not in ACTIVE_STATUSES:
            continue
        if attendance in INACTIVE_ATTENDANCE:
            continue
        date = _norm(row.get("appointmentDate") or row.get("date"))[:10]
        time = _norm(row.get("appointmentTime") or row.get("time"))[:5]
        if not date or not time:
            continue
        active.append(
            AppointmentSnapshot(
                id=_norm(row.get("id")),
                customer_name=_norm(row.get("authorizedPerson") or row.get("customerName") or row.get("name")),
                service=_norm(row.get("service")),
                date=date,
                time=time,
                status=status or "confirmed",
                source_appointment_id=_norm(row.get("sourceAppointmentId")) or None,
            )
        )
    active.sort(key=lambda a: (a.date, a.time))
    return CRMState(customer=customer, active_appointments=active, source="crm")


def extract_local_state_from_conversation(conversation: dict[str, Any]) -> CRMState:
    """Fallback snapshot from local conversation only. Not source-of-truth."""
    ig = _norm(conversation.get("instagram_user_id") or conversation.get("sender_id"))
    customer = CustomerSnapshot(
        id=ig,
        name=_norm(conversation.get("full_name") or conversation.get("lead_name")),
        phone=_norm(conversation.get("phone")),
        instagram_user_id=ig,
    ) if ig else None
    active = []
    if _norm(conversation.get("appointment_status")).lower() in ACTIVE_STATUSES and conversation.get("requested_date") and conversation.get("requested_time"):
        active.append(
            AppointmentSnapshot(
                id=_norm(conversation.get("appointment_id")),
                customer_name=_norm(conversation.get("full_name") or conversation.get("lead_name")),
                service=_norm(conversation.get("service")),
                date=_norm(conversation.get("requested_date"))[:10],
                time=_norm(conversation.get("requested_time"))[:5],
                status=_norm(conversation.get("appointment_status")) or "confirmed",
            )
        )
    return CRMState(customer=customer, active_appointments=active, source="local_fallback")
