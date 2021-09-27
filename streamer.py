import math

from benchmarks.NetworkStreamer import NetworkStreamer
from driver.benchmark import DataStreamer

if __name__ == '__main__':
    budget = 100_000_000
    generator = DataStreamer(16, math.ceil(budget / 3000), math.ceil(budget / 10000), budget)
    streamer = NetworkStreamer('localhost', 9019, generator)
    streamer.run()
