import argparse
import os
import sys

try:
    from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
    from pyflink.datastream.window import CountTumblingWindowAssigner
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False

from operators import ProcessLine, FilterByCity, TrackMinMax
from aggregators import WeatherDataAggregator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Real-Time PyFlink Weather Stream Processing & Windowed Aggregation Pipeline"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/sample_weather.jsonl",
        help="Path to newline-delimited JSON weather telemetry stream file.",
    )
    parser.add_argument(
        "--city",
        type=str,
        default="Sidney",
        help="Station city name to filter and aggregate (e.g., 'Sidney', 'Mobile', 'Birmingham').",
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=10,
        help="Count tumbling window size for metric aggregation.",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Flink job execution parallelism degree.",
    )
    return parser.parse_args()


def run_pipeline(input_path: str, city: str, window_size: int, parallelism: int):
    if not PYFLINK_AVAILABLE:
        print("[ERROR] Apache Flink (pyflink) is not installed in the current environment.")
        print("Install dependencies via: pip install -r requirements.txt")
        sys.exit(1)

    if not os.path.exists(input_path):
        print(f"[ERROR] Input telemetry dataset not found: {input_path}")
        sys.exit(1)

    abs_input_path = os.path.abspath(input_path)
    print(f"==================================================")
    print(f"      PyFlink Weather Stream Processing Job       ")
    print(f"==================================================")
    print(f"Source Dataset : {abs_input_path}")
    print(f"Station Filter : {city}")
    print(f"Window Spec    : CountTumblingWindow({window_size})")
    print(f"Parallelism    : {parallelism}")
    print(f"==================================================")

    # Initialize Flink Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(parallelism)

    # Instantiate Pipeline Operators
    op_clean = ProcessLine()
    op_filter = FilterByCity(city)
    op_track = TrackMinMax()

    # Define DataStream Transformation Graph
    pipeline = (
        env.read_text_file(abs_input_path)
        .map(op_clean.execute)
        .filter(op_filter.execute)
        .map(op_track.execute)
        .window_all(CountTumblingWindowAssigner.of(window_size))
        .aggregate(WeatherDataAggregator())
        .print()
    )

    env.execute(f"Weather-Stream-Aggregator-{city}")


def main():
    args = parse_args()
    run_pipeline(
        input_path=args.input,
        city=args.city,
        window_size=args.window_size,
        parallelism=args.parallelism,
    )


if __name__ == "__main__":
    main()
