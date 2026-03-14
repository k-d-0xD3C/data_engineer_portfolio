"""
Mortgage rate event schema.

This is the data contract for all events on the mortgage-rates-raw topic.
Any producer must conform to this schema. Any consumer can rely on it.
Schema changes require a version bump and backward compatibility review.
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json


SCHEMA_VERSION = "1.0.0"


@dataclass
class MortgageRateEvent:
    """
    Represents a single mortgage rate observation.

    Fields:
        event_id:         Unique identifier for deduplication
        schema_version:   Schema version for evolution tracking
        series_id:        FRED series identifier (e.g. MORTGAGE30US)
        series_name:      Human-readable series name
        rate:             Rate value as a percentage (e.g. 7.25)
        observation_date: Date the rate was observed (YYYY-MM-DD)
        published_at:     ISO8601 timestamp when FRED published the data
        simulated_at:     ISO8601 timestamp when simulator emitted the event
        source:           Data source identifier
        is_simulated:     Whether this is simulated or live data
        units:            Unit of measurement
        multiplier:       Multiplier applied to raw value
    """
    event_id: str
    schema_version: str
    series_id: str
    series_name: str
    rate: float
    observation_date: str
    published_at: str
    simulated_at: str
    source: str
    is_simulated: bool
    units: str = "Percent"
    multiplier: float = 1.0
    notes: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "MortgageRateEvent":
        data = json.loads(json_str)
        return cls(**data)

    def validate(self) -> list[str]:
        """
        Returns a list of validation errors.
        Empty list means the event is valid.
        """
        errors = []

        if not self.event_id:
            errors.append("event_id is required")
        if not self.series_id:
            errors.append("series_id is required")
        if not isinstance(self.rate, (int, float)):
            errors.append(f"rate must be numeric, got {type(self.rate)}")
        if self.rate <= 0 or self.rate > 30:
            errors.append(f"rate {self.rate} outside plausible range (0, 30]")
        if not self.observation_date:
            errors.append("observation_date is required")
        try:
            datetime.strptime(self.observation_date, "%Y-%m-%d")
        except ValueError:
            errors.append(f"observation_date {self.observation_date} must be YYYY-MM-DD")

        return errors


# Series metadata — defines what we ingest
FRED_SERIES = {
    "MORTGAGE30US": {
        "name": "30-Year Fixed Rate Mortgage Average",
        "units": "Percent",
        "frequency": "Weekly",
    },
    "MORTGAGE15US": {
        "name": "15-Year Fixed Rate Mortgage Average",
        "units": "Percent",
        "frequency": "Weekly",
    },
    "DGS10": {
        "name": "10-Year Treasury Constant Maturity Rate",
        "units": "Percent",
        "frequency": "Daily",
    },
}