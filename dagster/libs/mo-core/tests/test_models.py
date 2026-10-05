"""mo-core model tests: schema export + roundtrip."""

import json
from pathlib import Path

from mo_core.models import EXPORTED_MODELS, ExtractionRecord, export_json_schema


def test_extraction_record_roundtrip():
    record = ExtractionRecord(id="r1", source="dailymed", text="hello")
    data = json.loads(record.model_dump_json())
    assert ExtractionRecord.model_validate(data) == record


def test_export_json_schema_writes_all_models(tmp_path: Path):
    written = export_json_schema(tmp_path)
    names = {p.name for p in written}
    expected = {f"{m.__name__}.json" for m in EXPORTED_MODELS}
    assert names == expected
    for path in written:
        schema = json.loads(path.read_text())
        assert schema.get("type") == "object" or "properties" in schema
