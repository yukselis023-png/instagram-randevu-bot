with open("app/main.py", "r", encoding="utf-8") as f: lines = f.readlines()
for i, line in enumerate(lines):
    if "kw_clean = sanitize_text(kw).lower()" in line:
        # Find proper indent from previous line
        prev_indent = len(lines[i-1]) - len(lines[i-1].lstrip())
        lines[i] = (" " * (prev_indent + 4)) + "kw_clean = sanitize_text(kw).lower()\n"
    if 'if kw_clean in lowered and "fit_description" in svc:' in line:
        prev_indent = len(lines[i-2]) - len(lines[i-2].lstrip())
        lines[i] = (" " * (prev_indent + 4)) + 'if kw_clean in lowered and "fit_description" in svc:\n'
    if 'if kw_clean in lowered:\n' in line or 'if kw_clean in lowered:' in line:
        prev_indent = len(lines[i-2]) - len(lines[i-2].lstrip())
        lines[i] = (" " * (prev_indent + 4)) + 'if kw_clean in lowered:\n'

with open("app/main.py", "w", encoding="utf-8") as f: f.writelines(lines)
