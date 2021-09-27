import json

from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.streaming import StreamingContext


class WindowedAggregation:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sc = SparkContext("local[2]", "Windowed Aggregation Query")
        self.ssc = StreamingContext(self.sc, 4) # 4 second window as specified in the paper

    def run(self):
        query = self.ssc.socketTextStream(self.host, self.port) \
            .map(json.loads) \
            .map(lambda purchase: (str(purchase['packID']), purchase['price'])) \
            .reduceByKey(lambda a, b: a + b) \
            .pprint()

        self.ssc.start()
        self.ssc.awaitTermination()
        print(query)
