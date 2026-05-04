import re
with open("app/generic_core.py", "r", encoding="utf-8") as f:
    code = f.read()

bad_logic = """if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].strip()
            
        return json.loads(content)"""

new_logic = """match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        else:
            raise ValueError("No JSON found in response")"""

code = code.replace(bad_logic, new_logic)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(code)

