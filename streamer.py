import json
import socket
from typing import Union

from benchmarks.NetworkStreamer import NetworkStreamer
from driver.benchmark import DataStreamer

if __name__ == '__main__':
    budget = 1_000_000
    generator = DataStreamer(2, 100, budget)
    streamer = NetworkStreamer('localhost', 9017, generator)
    streamer.run()
