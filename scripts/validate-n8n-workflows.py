import json
import sys
from pathlib import Path


SAFE_FALLBACK = "Mesajınızı aldık, kontrol edip size en kısa sürede dönüş yapacağız."
ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / "workflows"


def load_workflow(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(f"{path.name}: JSON okunamadı: {exc}") from exc


def validate_message_bot(workflow: dict) -> list[str]:
    errors: list[str] = []
    node_names = {node.get("name") for node in workflow.get("nodes", [])}
    required_nodes = {
        "Webhook Messages",
        "Extract Incoming Messages",
        "Has Valid Message",
        "Process Booking Message",
        "Return Booking Response",
        "Handle Error",
    }
    missing = sorted(required_nodes - node_names)
    if missing:
        errors.append(f"instagram-message-bot: eksik node: {', '.join(missing)}")

    handle_error = next((node for node in workflow.get("nodes", []) if node.get("name") == "Handle Error"), None)
    code = ((handle_error or {}).get("parameters") or {}).get("jsCode", "")
    if SAFE_FALLBACK not in code:
        errors.append("instagram-message-bot: Handle Error güvenli fallback metnini dönmüyor")
    if "should_reply: true" not in code:
        errors.append("instagram-message-bot: Handle Error should_reply true değil")
    if "reply_text: null" in code or "error_no_reply" in code:
        errors.append("instagram-message-bot: Handle Error hâlâ cevapsız hata yolu içeriyor")

    connections = workflow.get("connections") or {}
    if "Process Booking Message" not in connections:
        errors.append("instagram-message-bot: Process Booking Message bağlantısı yok")
    return errors


def main() -> int:
    errors: list[str] = []
    message_bot = WORKFLOW_DIR / "instagram-message-bot.json"
    if not message_bot.exists():
        errors.append("instagram-message-bot.json bulunamadı")
    else:
        errors.extend(validate_message_bot(load_workflow(message_bot)))

    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    print("OK: n8n workflow doğrulaması geçti")
    return 0


if __name__ == "__main__":
    sys.exit(main())
