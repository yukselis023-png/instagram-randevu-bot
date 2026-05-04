import re
with open("app/generic_core.py", "r", encoding="utf-8") as f:
    code = f.read()

bad_invocation = """content = call_llm_content([{"role":"system","content":system_prompt}, {"role":"user","content":message_text}], is_json=True)
        return json.loads(content) if isinstance(content, str) else content"""

new_invocation = """content = call_llm_content(
            messages=[{"role":"system","content":system_prompt}, {"role":"user","content":message_text}],
            max_tokens=1000,
            temperature=0.0
        )
        if not content:
            raise ValueError("No content returned from LLM")
            
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].strip()
            
        return json.loads(content)"""

code = code.replace(bad_invocation, new_invocation)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(code)
