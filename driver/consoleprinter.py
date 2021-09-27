from driver.consumer import Consumer


class PrinterConsumer(Consumer):
    def update(self, data: []) -> None:
        print(data)
