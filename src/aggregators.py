from typing import Any, Dict

try:
    from pyflink.datastream.functions import AggregateFunction
except ImportError:
    # Graceful fallback for non-PyFlink test environments
    class AggregateFunction:
        """Fallback base class when PyFlink runtime is not installed."""
        pass


class WeatherDataAggregator(AggregateFunction):
    """
    Custom incremental window aggregation function.
    Maintains running accumulators for precipitation and wind speed across tumbling count/time windows.
    """

    def create_accumulator(self) -> Dict[str, Any]:
        """Initializes empty accumulator state."""
        return {
            "precipitation_sum": 0.0,
            "precipitation_count": 0,
            "precipitation_min": float("inf"),
            "precipitation_max": float("-inf"),
            "wind_speed_sum": 0.0,
            "wind_speed_count": 0,
            "wind_speed_min": float("inf"),
            "wind_speed_max": float("-inf"),
        }

    def add(self, value: Dict[str, Any], accumulator: Dict[str, Any]) -> Dict[str, Any]:
        """Incrementally consumes an incoming stream element into the window accumulator."""
        precip = value.get("Data.Precipitation", 0.0)
        wind = value.get("Data.Wind.Speed", 0.0)

        accumulator["precipitation_sum"] += precip
        accumulator["precipitation_count"] += 1
        accumulator["precipitation_min"] = min(accumulator["precipitation_min"], precip)
        accumulator["precipitation_max"] = max(accumulator["precipitation_max"], precip)

        accumulator["wind_speed_sum"] += wind
        accumulator["wind_speed_count"] += 1
        accumulator["wind_speed_min"] = min(accumulator["wind_speed_min"], wind)
        accumulator["wind_speed_max"] = max(accumulator["wind_speed_max"], wind)

        return accumulator

    def merge(self, a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        """Merges two partition accumulators during parallel distributed window evaluation."""
        return {
            "precipitation_sum": a["precipitation_sum"] + b["precipitation_sum"],
            "precipitation_count": a["precipitation_count"] + b["precipitation_count"],
            "precipitation_min": min(a["precipitation_min"], b["precipitation_min"]),
            "precipitation_max": max(a["precipitation_max"], b["precipitation_max"]),
            "wind_speed_sum": a["wind_speed_sum"] + b["wind_speed_sum"],
            "wind_speed_count": a["wind_speed_count"] + b["wind_speed_count"],
            "wind_speed_min": min(a["wind_speed_min"], b["wind_speed_min"]),
            "wind_speed_max": max(a["wind_speed_max"], b["wind_speed_max"]),
        }

    def get_result(self, accumulator: Dict[str, Any]) -> Dict[str, Any]:
        """Finalizes and projects aggregated window metrics upon window trigger."""
        precip_count = accumulator["precipitation_count"]
        wind_count = accumulator["wind_speed_count"]

        avg_precipitation = (accumulator["precipitation_sum"] / precip_count) if precip_count > 0 else 0.0
        avg_wind_speed = (accumulator["wind_speed_sum"] / wind_count) if wind_count > 0 else 0.0

        return {
            "avg_precipitation": round(avg_precipitation, 4),
            "min_precipitation": accumulator["precipitation_min"] if precip_count > 0 else 0.0,
            "max_precipitation": accumulator["precipitation_max"] if precip_count > 0 else 0.0,
            "avg_wind_speed": round(avg_wind_speed, 4),
            "min_wind_speed": accumulator["wind_speed_min"] if wind_count > 0 else 0.0,
            "max_wind_speed": accumulator["wind_speed_max"] if wind_count > 0 else 0.0,
            "total_records": precip_count,
        }
