import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

target = r'''def count_reply_sentences(reply_text: str | None) -> int:
    cleaned = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not cleaned:
        return 0
    parts = [part.strip() for part in re.split(r"[.!]+", cleaned) if part.strip()]
    return len(parts) or 1'''

replacement = r'''def count_reply_sentences(reply_text: str | None) -> int:
    cleaned = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not cleaned:
        return 0
    # Remove numbers with dots so they don't count as sentences (e.g. 12.900)
    cleaned = re.sub(r"\d+\.\d+", "NUM", cleaned)
    parts = [part.strip() for part in re.split(r"[.!]+", cleaned) if part.strip()]
    return len(parts) or 1'''

text = text.replace(target, replacement)

# ALSO fix that ugly "XXX PRICE QUESTION TRIGGERED XXX" print statement
text = text.replace('        print("XXX PRICE QUESTION TRIGGERED XXX")', '')

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

