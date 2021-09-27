import time
from multiprocessing import Queue

import numpy as np


class BenchmarkGenerator:
    users: list[int]
    prices: list[float]
    packs: dict
    pack_ids: list[int]

    def __init__(self):
        self.users = [i for i in range(10)]
        self.prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.packs = {i: self.prices[i] for i in range(len(self.prices))}
        self.pack_ids = [pack_id for pack_id in self.packs.keys()]

    def generate(self, queue: Queue, budget: int):
        """
        This function will fill the provided queue with purchase instances.

        :param queue: The queue that will be filled with purchase instances
        :param budget: The budget for this generator
        :return:
        """
        for _ in range(budget):
            queue.put(self.gen_purchase())

    def gen_purchase(self) -> (int, int, float, float):
        """
        This function wil generate a single instance for a purchase
        The values represent (userID, gemPackID, price, time)

        :rtype: (int, int, float, float)
        """
        user_id: int = np.random.choice(self.users)
        pack_id: int = np.random.choice(self.pack_ids)

        purchase = (
            user_id,
            pack_id,
            self.packs[pack_id],
            time.time()
        )
        return purchase
