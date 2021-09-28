import argparse
import math

from benchmarks.NetworkStreamer import NetworkStreamer
from driver.benchmark import DataStreamer

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9999)
    parser.add_argument('--budget', type=int, default=100_000_000)
    parser.add_argument('--rate', type=int, default=3000, help='rate in transactions per second')
    parser.add_argument('--generators', type=int, default=16, help='number of generators')
    parser.add_argument('--max_queue_size', type=int, default=10_000,
                        help='maximum queue size for the generated transactions')

    args = parser.parse_args()

    generator = DataStreamer(args.generators,
                             math.ceil(args.budget / args.rate),
                             math.ceil(args.budget / args.max_queue_size),
                             args.budget)
    streamer = NetworkStreamer(args.host, args.port, generator)
    streamer.run()
