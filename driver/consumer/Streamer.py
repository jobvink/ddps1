from abc import ABC, abstractmethod

from driver.consumer import Consumer


class Streamer(ABC):
    """
    The Streamer interface declares a set of methods for managing subscribers.
    """

    @abstractmethod
    def attach(self, consumer: Consumer) -> None:
        """
        Attach an Consumer to the Streamer.
        """
        pass

    @abstractmethod
    def detach(self, consumer: Consumer) -> None:
        """
        Detach an Consumer from the Streamer.
        """
        pass

    @abstractmethod
    def notify(self) -> None:
        """
        Notify all Consumers about an event.
        """
        pass