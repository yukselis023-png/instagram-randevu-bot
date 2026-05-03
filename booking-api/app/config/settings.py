import json
import os

def load_business_profile():
    profile = os.getenv("BUSINESS_PROFILE", "doel")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{profile}.json")
    if not os.path.exists(file_path):
        file_path = os.path.join(current_dir, "doel.json")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print("Config load error:", e)
        return {"business_name": "Generic Business", "service_catalog": []}

CONFIG = load_business_profile()

def get_config():
    return CONFIG
