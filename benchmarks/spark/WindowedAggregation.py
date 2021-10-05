import json
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


class WindowedAggregation:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, master, host, port, storage):
        self.master = master
        self.host = host
        self.port = port
        self.storage = storage
        self.sc = SparkContext(self.master, "Windowed Aggregation Query")
        self.sc.addPyFile('/var/scratch/ddps2105/ddps1/benchmarks/spark/WindowedAggregation.py')
        self.ssc = StreamingContext(self.sc, 4)  # 4 second window as specified in the paper

    @staticmethod
    def aggregate(a, b) -> (float, float):
        """
        This function returns the sum of the price and the max of the event_time
        :rtype: (float, float)
        """
        a = (0, 0) if a is None else a
        b = (0, 0) if b is None else b
        return a[0] + b[0], max(a[1], b[1])

    def run(self):

        self.ssc.socketTextStream(self.host, self.port) \
            .map(json.loads) \
            .map(lambda purchase: (str(purchase['packID']), (purchase['price'], purchase['event_time']))) \
            .reduceByKey(self.aggregate) \
            .map(lambda aggregated_result: {'packID': aggregated_result[0],
                                            'price': aggregated_result[1][0],
                                            'latency': time.time() - aggregated_result[1][1]}) \
            .saveAsTextFiles(self.storage)

        self.ssc.start()
        self.ssc.awaitTermination()
