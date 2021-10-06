import argparse
import json
import os.path
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.streaming import StreamingContext


class WindowedJoin:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, master, host, port, storage):
        self.master = master
        self.host = host
        self.port = port
        self.storage = storage

        self.sc = SparkContext(self.master, "Windowed Aggregation Query")
        # self.sc.addPyFile('/var/scratch/ddps2105/ddps1/join.py')
        self.ssc = StreamingContext(self.sc, 4)  # 4 second window as specified in the paper

        self.purchase_schema = StructType([
            StructField('userID', IntegerType(), False),
            StructField('gemPackID', IntegerType(), False),
            StructField('price', FloatType(), True),
            StructField('time', FloatType(), False),
        ])

        self.ad_schema = StructType([
            StructField('userID', IntegerType(), False),
            StructField('gemPackID', IntegerType(), False),
            StructField('time', FloatType(), False),
        ])

    def run(self):
        """
        Windowed Join Query
        SELECT  p.userID, p.gemPackID, p.price
        FROM    PURCHASES[Ranger, Slides] as p,
                ADS[Ranger, Slides] as a,
        WHERE   p.userID = a.userID AND
                p.gemPackID = a.gemPackID
        """

        lines = self.ssc.socketTextStream(self.host, self.port)

        purchases: DataFrame = lines \
            .filter(lambda line: 'price' in line) \
            .map(json.loads) \
            .map(lambda x: ('u:' + str(x['userID']) + ',g:' + str(x['gemPackID']), x))

        ads = lines \
            .filter(lambda line: 'price' not in line) \
            .map(json.loads) \
            .map(lambda x: ('u:' + str(x['userID']) + ',g:' + str(x['gemPackID']), x))

        purchases \
            .join(ads) \
            .map(lambda record: {"userID": record[1][0]['userID'],
                                 "gemPackID:": record[1][1]['gemPackID'],
                                 "p.time": record[1][0]['time'],
                                 "a.time": record[1][1]['time'],
                                 "time": max(record[1][0]['time'], record[1][1]['time']),
                                 "latency": time.time() - max(record[1][0]['time'], record[1][1]['time']),
                                 }) \
            .pprint()

        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9999)
    parser.add_argument('--storage', type=str, default='dps1/results/windowed_aggregation/')
    parser.add_argument('--master', type=str, required=True,
                        help='the name of the spark master node (example: spark://node102.cm.cluster:7077)')

    args = parser.parse_args()

    os.makedirs(args.storage, exist_ok=True)

    aggregation = WindowedJoin(args.master, args.host, args.port, args.storage)
    aggregation.run()
