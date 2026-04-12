
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion_engine import FileFamily
from upload_engine import _family_to_tipo

def test_family_to_tipo_resources():
    assert _family_to_tipo(FileFamily.RISORSE) == 'risorse'

def test_family_to_tipo_orders_detail_not_resources():
    assert _family_to_tipo(FileFamily.ORDERS_DETAIL) != 'risorse'
