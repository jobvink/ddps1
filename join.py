import argparse
import json
import os.path
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.streaming import StreamingContext


class WindowedJoin:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, master, host, port, storage):
        self.master = master
        self.host = host
        self.port = port
        self.storage = storage

        self.sc = SparkContext(self.master, "Windowed Join Query")
        self.sc.addPyFile('./join.py')
        self.ssc = StreamingContext(self.sc, 4)  # 4 second window as specified in the paper

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
            .filter(lambda record: record[1][0]['userID'] == record[1][1]['userID'] and
                                   record[1][0]['gemPackID'] == record[1][1]['gemPackID']) \
            .map(lambda record: {"userID": record[1][0]['userID'],
                                 "gemPackID:": record[1][1]['gemPackID'],
                                 "p.time": record[1][0]['time'],
                                 "a.time": record[1][1]['time'],
                                 "time": time.time(),
                                 "latency": time.time() - max(record[1][0]['time'], record[1][1]['time']),
                                 }) \
            .saveAsTextFiles(self.storage)

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
