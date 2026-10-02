import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Operator(ABC):
    """Abstract base class for streaming data transformations."""

    @abstractmethod
    def execute(self, record: Any) -> Any:
        pass


class ProcessLine(Operator):
    """Sanitizes raw incoming stream lines."""

    def execute(self, line: str) -> str:
        return line.strip()


class FilterByCity(Operator):
    """Filters JSON telemetry records by station city."""

    def __init__(self, target_city: str):
        self.target_city = target_city

    def execute(self, record: str) -> bool:
        try:
            data = json.loads(record)
            return data.get("Station.City") == self.target_city
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Skipping malformed stream record: %s (error: %s)", record, e)
            return False


class TrackMinMax(Operator):
    """Stateful operator tracking running historical extrema for meteorological metrics."""

    def __init__(self):
        self.min_wind_speed = float("inf")
        self.max_wind_speed = float("-inf")
        self.min_precipitation = float("inf")
        self.max_precipitation = float("-inf")

    def execute(self, record: str) -> Dict[str, Any]:
        data = json.loads(record) if isinstance(record, str) else record

        wind_speed = data.get("Data.Wind.Speed")
        if wind_speed is not None:
            self.min_wind_speed = min(self.min_wind_speed, wind_speed)
            self.max_wind_speed = max(self.max_wind_speed, wind_speed)

        precipitation = data.get("Data.Precipitation")
        if precipitation is not None:
            self.min_precipitation = min(self.min_precipitation, precipitation)
            self.max_precipitation = max(self.max_precipitation, precipitation)

        data["MinWindSpeed"] = self.min_wind_speed
        data["MaxWindSpeed"] = self.max_wind_speed
        data["MinPrecipitation"] = self.min_precipitation
        data["MaxPrecipitation"] = self.max_precipitation
        return data
