from abc import ABC, abstractmethod


class Consumer(ABC):
    """
    The Consumer interface declares the update method, used by Streamers.
    """

    @abstractmethod
    def update(self, data: []) -> None:
        """
        Receive update from Streamer.
        """
        pass