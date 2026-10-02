import json
import sys
import unittest

sys.path.insert(0, "src")

from operators import ProcessLine, FilterByCity, TrackMinMax
from aggregators import WeatherDataAggregator


class TestPyFlinkWeatherPipeline(unittest.TestCase):
    def test_process_line(self):
        op = ProcessLine()
        raw = "  {\"test\": 123}  \n"
        self.assertEqual(op.execute(raw), "{\"test\": 123}")

    def test_filter_by_city(self):
        op_sidney = FilterByCity("Sidney")
        matching = json.dumps({"Station.City": "Sidney", "Data.Temperature.Avg Temp": 45})
        non_matching = json.dumps({"Station.City": "Berlin", "Data.Temperature.Avg Temp": 20})
        malformed = "not valid json {"

        self.assertTrue(op_sidney.execute(matching))
        self.assertFalse(op_sidney.execute(non_matching))
        self.assertFalse(op_sidney.execute(malformed))

    def test_track_min_max_stateful(self):
        tracker = TrackMinMax()

        # First event
        ev1 = tracker.execute(json.dumps({"Data.Wind.Speed": 12.0, "Data.Precipitation": 0.5}))
        self.assertEqual(ev1["MinWindSpeed"], 12.0)
        self.assertEqual(ev1["MaxWindSpeed"], 12.0)
        self.assertEqual(ev1["MinPrecipitation"], 0.5)
        self.assertEqual(ev1["MaxPrecipitation"], 0.5)

        # Second event (lower wind, higher precip)
        ev2 = tracker.execute(json.dumps({"Data.Wind.Speed": 8.5, "Data.Precipitation": 1.2}))
        self.assertEqual(ev2["MinWindSpeed"], 8.5)
        self.assertEqual(ev2["MaxWindSpeed"], 12.0)
        self.assertEqual(ev2["MinPrecipitation"], 0.5)
        self.assertEqual(ev2["MaxPrecipitation"], 1.2)

        # Third event (higher wind, lower precip)
        ev3 = tracker.execute(json.dumps({"Data.Wind.Speed": 25.0, "Data.Precipitation": 0.1}))
        self.assertEqual(ev3["MinWindSpeed"], 8.5)
        self.assertEqual(ev3["MaxWindSpeed"], 25.0)
        self.assertEqual(ev3["MinPrecipitation"], 0.1)
        self.assertEqual(ev3["MaxPrecipitation"], 1.2)

    def test_weather_aggregator_lifecycle(self):
        agg = WeatherDataAggregator()
        acc = agg.create_accumulator()

        records = [
            {"Data.Precipitation": 0.2, "Data.Wind.Speed": 10.0},
            {"Data.Precipitation": 0.4, "Data.Wind.Speed": 20.0},
            {"Data.Precipitation": 0.6, "Data.Wind.Speed": 30.0},
        ]

        for r in records:
            acc = agg.add(r, acc)

        res = agg.get_result(acc)
        self.assertEqual(res["total_records"], 3)
        self.assertEqual(res["avg_precipitation"], 0.4)
        self.assertEqual(res["min_precipitation"], 0.2)
        self.assertEqual(res["max_precipitation"], 0.6)
        self.assertEqual(res["avg_wind_speed"], 20.0)
        self.assertEqual(res["min_wind_speed"], 10.0)
        self.assertEqual(res["max_wind_speed"], 30.0)

    def test_weather_aggregator_merge(self):
        agg = WeatherDataAggregator()
        acc1 = agg.create_accumulator()
        acc2 = agg.create_accumulator()

        acc1 = agg.add({"Data.Precipitation": 0.1, "Data.Wind.Speed": 15.0}, acc1)
        acc2 = agg.add({"Data.Precipitation": 0.9, "Data.Wind.Speed": 25.0}, acc2)

        merged = agg.merge(acc1, acc2)
        res = agg.get_result(merged)

        self.assertEqual(res["total_records"], 2)
        self.assertEqual(res["avg_precipitation"], 0.5)
        self.assertEqual(res["min_precipitation"], 0.1)
        self.assertEqual(res["max_precipitation"], 0.9)
        self.assertEqual(res["avg_wind_speed"], 20.0)
        self.assertEqual(res["min_wind_speed"], 15.0)
        self.assertEqual(res["max_wind_speed"], 25.0)


if __name__ == "__main__":
    unittest.main()
