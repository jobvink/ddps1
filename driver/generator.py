import time
from multiprocessing import Queue

import numpy as np


class BenchmarkGenerator:

    def generate(self, queue: Queue, budget: int):
        """
        This function will fill the provided queue with purchase instances.

        :param queue: The queue that will be filled with purchase instances
        :param budget: The budget for this generator
        :return:
        """
        for _ in range(budget):
            queue.put(self.gen_purchase())

    @staticmethod
    def gen_purchase() -> (float, float, float):
        """
        This function wil generate a single instance for a purchase
        :rtype: (float, float, float)
        """
        purchase = (
            np.random.normal(),
            np.random.uniform(),
            time.time()
        )
        return purchase
