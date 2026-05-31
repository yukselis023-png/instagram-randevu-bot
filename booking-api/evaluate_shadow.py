import os
import json

def test_shadow_logging():
    os.environ["ANSWER_FIRST_PIPELINE"] = "shadow"
    import pytest
    pytest.main(["-q", "booking-api/tests"])

if __name__ == "__main__":
    test_shadow_logging()
