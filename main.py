import argparse
import os.path

from benchmarks.spark.WindowedAggregation import WindowedAggregation

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9999)
    parser.add_argument('--storage', type=str, default='dps1/results/windowed_aggregation/')

    args = parser.parse_args()

    os.makedirs(args.storage, exist_ok=True)

    aggregation = WindowedAggregation(args.host, args.port, args.storage)
    aggregation.run()
