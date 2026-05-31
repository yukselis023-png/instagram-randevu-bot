import app.generic_core as gc
from app.main import extract_time_for_state, extract_time


def test_plain_user_number_is_not_time():
    text = "Merhaba web tasarım için bilgi almak istiyorum kullanıcı 5"
    assert extract_time_for_state(text, "new") is None
    assert extract_time(text) is None
    assert gc.extract_generic_datetime_time(text) is None
