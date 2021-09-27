from benchmarks.dev.consoleprinter import PrinterConsumer
from driver.benchmark import DataStreamer

if __name__ == '__main__':
    consumer = PrinterConsumer()
    streamer = DataStreamer(consumer, 2, 100, 10)
    streamer.run()
