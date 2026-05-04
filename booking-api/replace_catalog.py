with open("app/main.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
in_catalog = False
for line in lines:
    if line.startswith("DOEL_SERVICE_CATALOG = ["):
        in_catalog = True
        new_lines.append("DOEL_SERVICE_CATALOG = get_config().get('service_catalog', [])\n")
        continue
    
    if in_catalog:
        if line.startswith("]"):
            in_catalog = False
        continue
        
    new_lines.append(line)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.writelines(new_lines)
