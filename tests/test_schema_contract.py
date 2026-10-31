import hashlib
import json
from pathlib import Path


def test_schema_contract_checksums_match_bundled_and_local_authority():
    contract = Path(__file__).parent / "schema_contract"
    authority = Path(__file__).resolve().parents[2] / "fin/fin-db"
    forward = {
        "sql/17_scheduled_engine_v4.5/01_expand_silver.sql": "sql/20_scheduled_engine_v4.5/01_expand_silver.sql",
        "sql/17_scheduled_engine_v4.5/03_filtered_workers.sql": "sql/20_scheduled_engine_v4.5/02_filtered_workers.sql",
    }
    for relative, expected in json.loads((contract / "manifest.json").read_text()).items():
        assert hashlib.sha256((contract / relative).read_bytes()).hexdigest() == expected
        # The legacy stage-14 fixture intentionally retains operational seed rows.
        current = forward.get(relative)
        if current and (authority / current).exists():
            assert hashlib.sha256((authority / current).read_bytes()).hexdigest() == expected
