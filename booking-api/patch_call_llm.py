import re

with open("app/generic_core.py", "r", encoding="utf-8") as f:
    code = f.read()

import_str = """import json
import re
import datetime
import requests
import os"""

if "import requests" not in code:
    code = code.replace("import json\nimport re\nimport datetime", import_str)

new_func = """def call_llm_json(system_prompt: str, user_text: str) -> dict:
    import requests, os
    llm_url = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1")
    llm_key = os.getenv("LLM_API_KEY", "")
    llm_model = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    
    if not llm_key:
        from app.main import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL
        llm_url = LLM_BASE_URL
        llm_key = LLM_API_KEY
        llm_model = LLM_MODEL

    headers = {"Authorization": f"Bearer {llm_key}", "Content-Type": "application/json"}
    payload = {
        "model": llm_model,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_text}],
        "temperature": 0.0,
        "max_tokens": 1000
    }
    
    try:
        resp = requests.post(f"{llm_url}/chat/completions", headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception as e:
        raise ValueError(f"LLM JSON Error: {e}")
"""

# Inject before function
if "def call_llm_json" not in code:
    code = code.replace("def invoke_generic_llm(", new_func + "\n\ndef invoke_generic_llm(")

old_call = """content = call_llm_content(
            messages=[{"role":"system","content":system_prompt}, {"role":"user","content":message_text}],
            max_tokens=1000,
            temperature=0.0
        )
        if not content:
            raise ValueError("No content returned from LLM")
            
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        else:
            raise ValueError("No JSON found in response")"""

new_call = """return call_llm_json(system_prompt, message_text)"""

code = code.replace(old_call, new_call)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(code)

