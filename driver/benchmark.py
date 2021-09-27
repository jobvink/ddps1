import math
from multiprocessing import Process, Queue

from driver.generator import BenchmarkGenerator
from driver.consumer import Consumer


class DataStreamer:
    """
    This class initializes the data generator and provides data for the Software Under Test (SUT)

    :param consumer: the consumer that handles the data processing
    :param n_generators: number of generators that will fill the queue
    :param max_queue_size: maximum size of the queue
    :param budget: this budged will be distributed over the generators
    """
    n_generators: int
    queue: Queue
    generators: []
    consumer: Consumer

    def __init__(self, consumer: Consumer, n_generators: int, max_queue_size: int, budget: int):
        self.consumer = consumer
        self.n_generators = n_generators
        self.budget = budget
        self.queue = Queue(max_queue_size)

        # generator for the benchmark
        generator = BenchmarkGenerator()

        # These generators wil fill up the queue with purchase instances
        # in there own separate threads. Every generator generates an equal
        # amount of instances. Note: the generator may go over budget because
        # the budget is divided into equal parts and rounded up.
        self.generators = [
            Process(
                target=generator.generate,
                args=(self.queue, math.ceil(self.budget / self.n_generators)),
                daemon=True
            )
            for _ in range(self.n_generators)
        ]

    def run(self) -> None:
        """
        Starts the generator and runs the benchmark
        """
        for generator in self.generators:
            generator.start()

        for i in range(self.budget):
            data = self.queue.get()
            self.consumer.update(data)
