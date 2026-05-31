import sys, pathlib
root=pathlib.Path(__file__).resolve().parents[2] / 'booking-api'
sys.path.insert(0,str(root))
from app.main import app
