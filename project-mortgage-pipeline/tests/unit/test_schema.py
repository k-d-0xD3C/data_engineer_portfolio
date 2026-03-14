"""Unit tests for the MortgageRateEvent schema and validation."""
import json
import pytest
from datetime import datetime, timezone
from src.kafka.schema import MortgageRateEvent, SCHEMA_VERSION


def make_valid_event(**overrides) -> MortgageRateEvent:
    defaults = dict(
        event_id="test-id-123",
        schema_version=SCHEMA_VERSION,
        series_id="MORTGAGE30US",
        series_name="30-Year Fixed Rate Mortgage Average",
        rate=7.25,
        observation_date="2024-01-05",
        published_at="2024-01-05",
        simulated_at=datetime.now(timezone.utc).isoformat(),
        source="FRED",
        is_simulated=True,
    )
    defaults.update(overrides)
    return MortgageRateEvent(**defaults)


class TestMortgageRateEventValidation:

    def test_valid_event_has_no_errors(self):
        event = make_valid_event()
        assert event.validate() == []

    def test_missing_event_id_fails(self):
        event = make_valid_event(event_id="")
        errors = event.validate()
        assert any("event_id" in e for e in errors)

    def test_rate_below_zero_fails(self):
        event = make_valid_event(rate=-1.0)
        errors = event.validate()
        assert any("rate" in e for e in errors)

    def test_rate_above_30_fails(self):
        event = make_valid_event(rate=31.0)
        errors = event.validate()
        assert any("rate" in e for e in errors)

    def test_invalid_date_format_fails(self):
        event = make_valid_event(observation_date="01/05/2024")
        errors = event.validate()
        assert any("observation_date" in e for e in errors)

    def test_zero_rate_fails(self):
        event = make_valid_event(rate=0.0)
        errors = event.validate()
        assert any("rate" in e for e in errors)


class TestMortgageRateEventSerialization:

    def test_round_trip_serialization(self):
        event = make_valid_event()
        json_str = event.to_json()
        restored = MortgageRateEvent.from_json(json_str)
        assert restored.event_id == event.event_id
        assert restored.rate == event.rate
        assert restored.series_id == event.series_id

    def test_to_json_produces_valid_json(self):
        event = make_valid_event()
        json_str = event.to_json()
        parsed = json.loads(json_str)
        assert parsed["series_id"] == "MORTGAGE30US"
        assert parsed["rate"] == 7.25