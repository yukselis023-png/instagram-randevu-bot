import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Replace the beginning of process_instagram_message to inject the feature flag
target = '''@app.post("/api/process-instagram-message", response_model=ProcessResult)
def process_instagram_message(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    request_started_at = time_module.perf_counter()'''

replacement = '''@app.post("/api/process-instagram-message", response_model=ProcessResult)
def process_instagram_message(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    import os
    if os.getenv("CHATBOT_ENGINE") == "generic":
        from app.generic_core import process_instagram_message_generic
        return process_instagram_message_generic(payload, background_tasks)

    request_started_at = time_module.perf_counter()'''

if target in text:
    text = text.replace(target, replacement)
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Hook success.")
else:
    print("Target not found.")
