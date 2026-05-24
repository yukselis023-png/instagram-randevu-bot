"""Analytics aggregation for AI Agent dashboard.

Aggregates:
- Conversations per channel (tenant-aware)
- Appointment conversion rate
- Lead score distribution
- Handoff rate
- Follow-up stats
"""
from __future__ import annotations
import json, logging
from typing import Any
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("analytics")


def aggregate_tenant_stats(conn: Any) -> dict[str, Any]:
    """Aggregate platform-wide and per-tenant stats."""
    if not conn:
        return {"ok": False, "error": "no_db"}
    try:
        with conn.cursor() as cur:
            # ── Per-tenant conversation stats ──
            cur.execute("""
                SELECT
                    tenant_slug,
                    COUNT(*) AS total_convos,
                    COUNT(*) FILTER (WHERE state NOT IN ('new', 'ignored', 'completed')) AS active,
                    COUNT(*) FILTER (WHERE state = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE assigned_human = TRUE) AS handoffs
                FROM conversations
                GROUP BY tenant_slug
                ORDER BY tenant_slug
            """)
            conv_rows = cur.fetchall()

            # ── Appointment stats ──
            cur.execute("""
                SELECT
                    tenant_slug,
                    COUNT(*) AS total_appts,
                    COUNT(*) FILTER (WHERE status IN ('scheduled', 'confirmed')) AS upcoming,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                    COUNT(*) FILTER (WHERE status = 'no_show') AS no_show
                FROM appointments
                GROUP BY tenant_slug
                ORDER BY tenant_slug
            """)
            apt_rows = cur.fetchall()
            apt_map = {r["tenant_slug"]: r for r in apt_rows}

            # ── Lead score stats (from customers) ──
            cur.execute("""
                SELECT
                    tenant_slug,
                    COUNT(*) AS total,
                    AVG(lead_score)::numeric(5,1) AS avg_score,
                    COUNT(*) FILTER (WHERE lead_score >= 70) AS hot,
                    COUNT(*) FILTER (WHERE lead_score >= 40 AND lead_score < 70) AS warm,
                    COUNT(*) FILTER (WHERE lead_score < 40) AS cold
                FROM customers
                WHERE lead_score IS NOT NULL
                GROUP BY tenant_slug
                ORDER BY tenant_slug
            """)
            score_rows = cur.fetchall()
            score_map = {r["tenant_slug"]: r for r in score_rows}

            # ── Channel breakdown ──
            cur.execute("""
                SELECT tenant_slug, platform, COUNT(*) AS msg_count
                FROM message_logs
                WHERE created_at > NOW() - INTERVAL '30 days'
                GROUP BY tenant_slug, platform
                ORDER BY tenant_slug, platform
            """)
            channel_rows = cur.fetchall()

            # ── Build per-tenant stats ──
            tenants: dict[str, dict] = {}
            for r in conv_rows:
                slug = r["tenant_slug"] or "default"
                tenants[slug] = {
                    "conversations": {
                        "total": r["total_convos"],
                        "active": r["active"],
                        "completed": r["completed"],
                        "handoffs": r["handoffs"],
                    },
                    "appointments": {"total": 0, "upcoming": 0, "completed": 0, "cancelled": 0, "no_show": 0},
                    "lead_scores": {"total": 0, "avg_score": None, "hot": 0, "warm": 0, "cold": 0},
                    "channels": {},
                }
            for slug, data in tenants.items():
                a = apt_map.get(slug, {})
                data["appointments"] = {
                    "total": a.get("total_appts", 0),
                    "upcoming": a.get("upcoming", 0),
                    "completed": a.get("completed", 0),
                    "cancelled": a.get("cancelled", 0),
                    "no_show": a.get("no_show", 0),
                }
                s = score_map.get(slug, {})
                data["lead_scores"] = {
                    "total": s.get("total", 0),
                    "avg_score": float(s["avg_score"]) if s.get("avg_score") else None,
                    "hot": s.get("hot", 0),
                    "warm": s.get("warm", 0),
                    "cold": s.get("cold", 0),
                }
                # Conversion rate
                total_appts = data["appointments"]["total"]
                total_convos = data["conversations"]["total"]
                data["conversion_rate"] = round(total_appts / total_convos * 100, 1) if total_convos > 0 else 0.0

            for r in channel_rows:
                slug = r["tenant_slug"] or "default"
                plat = r["platform"] or "unknown"
                if slug in tenants:
                    tenants[slug]["channels"][plat] = r["msg_count"]

            # ── Platform totals ──
            total_convos = sum(t["conversations"]["total"] for t in tenants.values())
            total_appts = sum(t["appointments"]["total"] for t in tenants.values())
            total_handoffs = sum(t["conversations"]["handoffs"] for t in tenants.values())

            return {
                "ok": True,
                "platform": {
                    "tenants": len(tenants),
                    "total_conversations": total_convos,
                    "total_appointments": total_appts,
                    "total_handoffs": total_handoffs,
                    "overall_conversion": round(total_appts / total_convos * 100, 1) if total_convos > 0 else 0.0,
                },
                "per_tenant": tenants,
            }
    except Exception as exc:
        logger.error("analytics_error %s", exc)
        return {"ok": False, "error": str(exc)}


def aggregate_time_series(conn: Any, days: int = 30) -> dict[str, Any]:
    """Appointments/messages over time for a chart."""
    if not conn:
        return {"ok": False, "error": "no_db"}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE(created_at) as dt,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status IN ('scheduled', 'confirmed')) AS booked
                FROM appointments
                WHERE created_at > NOW() - INTERVAL '%s days'
                GROUP BY dt
                ORDER BY dt
            """, (days,))
            appointments = [{"date": str(r["dt"]), "total": r["total"], "booked": r["booked"]} for r in cur.fetchall()]

            cur.execute("""
                SELECT
                    DATE(created_at) as dt,
                    COUNT(*) AS total
                FROM message_logs
                WHERE created_at > NOW() - INTERVAL '%s days'
                GROUP BY dt
                ORDER BY dt
            """, (days,))
            messages = [{"date": str(r["dt"]), "total": r["total"]} for r in cur.fetchall()]

            return {
                "ok": True,
                "appointments": appointments,
                "messages": messages,
            }
    except Exception as exc:
        logger.error("timeseries_error %s", exc)
        return {"ok": False, "error": str(exc)}
