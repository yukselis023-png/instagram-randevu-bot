from __future__ import annotations

import re
import unicodedata
from typing import Any


DEFAULT_FORBIDDEN_CUES = [
    "mesajinizi dikkate",
    "mesajınızı dikkate",
    "dogrudan cevap vereyim",
    "doğrudan cevap vereyim",
    "anlasilmadi",
    "anlaşılmadı",
    "lutfen daha acik",
    "lütfen daha açık",
    "daha fazla bilgi almak ister misiniz",
    "bir sonraki adimimiz ne olacak",
    "bir sonraki adımımız ne olacak",
    "needed bilgi",
    "needed bilgiler",
]


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.translate(
        str.maketrans(
            {
                "İ": "I",
                "ı": "i",
                "Ş": "S",
                "ş": "s",
                "Ğ": "G",
                "ğ": "g",
                "Ü": "U",
                "ü": "u",
                "Ö": "O",
                "ö": "o",
                "Ç": "C",
                "ç": "c",
            }
        )
    )
    return re.sub(r"\s+", " ", text).strip().lower()


def result_reply(result: dict[str, Any]) -> str:
    return str(result.get("reply_text") or result.get("response") or result.get("message") or "")


def result_metrics(result: dict[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics")
    if isinstance(metrics, dict):
        return metrics
    nested = result.get("normalized")
    if isinstance(nested, dict) and isinstance(nested.get("metrics"), dict):
        return nested["metrics"]
    return {}


def assert_quality_reply(
    case_name: str,
    message: str,
    result: dict[str, Any],
    *,
    expected_any: list[str] | None = None,
    forbidden: list[str] | None = None,
    require_should_reply: bool = True,
) -> None:
    reply = result_reply(result)
    normalized_reply = normalize_text(reply)
    normalized_message = normalize_text(message)
    forbidden_cues = [*DEFAULT_FORBIDDEN_CUES, *(forbidden or [])]

    if require_should_reply:
        assert result.get("should_reply") is True, f"{case_name}: should_reply false for {message!r}"
    assert normalized_reply, f"{case_name}: empty reply for {message!r}"
    assert normalized_reply != normalized_message, f"{case_name}: reply repeats the inbound message"

    for cue in forbidden_cues:
        assert normalize_text(cue) not in normalized_reply, f"{case_name}: forbidden cue {cue!r} in reply {reply!r}"

    if expected_any:
        normalized_expected = [normalize_text(item) for item in expected_any]
        assert any(item in normalized_reply for item in normalized_expected), (
            f"{case_name}: reply {reply!r} does not contain any expected cue {expected_any!r}"
        )


def assert_no_repeated_replies(results: list[dict[str, Any]]) -> None:
    previous = ""
    for index, result in enumerate(results):
        reply = normalize_text(result_reply(result))
        assert reply, f"result {index}: empty reply"
        assert reply != previous, f"result {index}: repeated reply {result_reply(result)!r}"
        previous = reply


def assert_booking_progression(
    case_name: str,
    results: list[dict[str, Any]],
    *,
    required_stages: list[str],
) -> None:
    stages: list[str] = []
    for result in results:
        metrics = result_metrics(result)
        stage = result.get("booking_stage") or metrics.get("booking_stage")
        if stage:
            stages.append(str(stage))

    cursor = 0
    for stage in stages:
        if cursor < len(required_stages) and stage == required_stages[cursor]:
            cursor += 1

    assert cursor == len(required_stages), (
        f"{case_name}: booking stages {stages!r} did not include required progression {required_stages!r}"
    )
