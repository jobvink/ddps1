import argparse
import os.path

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, when, col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.streaming import StreamingContext


class WindowedAggregation:
    ssc: StreamingContext
    spark: SparkSession

    def __init__(self, master, host, port, storage):
        self.master = master
        self.host = host
        self.port = port
        self.storage = storage

        self.spark = SparkSession \
            .builder \
            .master(self.master) \
            .appName("Windowed Aggregation Query") \
            .getOrCreate()

        self.lines = self.spark \
            .readStream \
            .format('socket') \
            .option('host', self.host) \
            .option('port', self.port) \
            .load()

        self.purchase_schema = StructType([
            StructField('userId', IntegerType(), False),
            StructField('gemPackID', IntegerType(), False),
            StructField('price', FloatType(), True),
            StructField('time', FloatType(), False),
        ])

        self.ad_schema = StructType([
            StructField('userId', IntegerType(), False),
            StructField('gemPackID', IntegerType(), False),
            StructField('time', FloatType(), False),
        ])

    @staticmethod
    def aggregate(a, b) -> (float, float):
        """
        This function returns the sum of the price and the max of the event time
        :rtype: (float, float)
        """
        a = (0, 0) if a is None else a
        b = (0, 0) if b is None else b
        return a[0] + b[0], max(a[1], b[1])

    def run(self):
        """
        Windowed Join Query
        SELECT  p.userID, p.gemPackID, p.price
        FROM    PURCHASES[Ranger, Slides] as p,
                ADS[Ranger, Slides] as a,
        WHERE   p.userID = a.userID AND
                p.gemPackID = a.gemPackID
        """

        purchases: DataFrame = self.lines \
            .selectExpr('CAST(value AS STRING) value') \
            .where(col('value').like('%price%')) \
            .select(from_json(col('value'), schema=self.purchase_schema).alias('p')) \
            .select('p.*')

        ads: DataFrame = self.lines \
            .selectExpr('CAST(value AS STRING) value') \
            .where(~col('value').like('%price%')) \
            .select(from_json(col('value'), schema=self.purchase_schema).alias('a')) \
            .select('a.*')

        query = purchases \
            .join(ads, [purchases.userId == ads.userId, purchases.gemPackID == ads.gemPackID]) \
            .writeStream \
            .format('console') \
            .trigger(processingTime='2 seconds') \
            .option('truncate', 'False') \
            .start()

        query.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed data processing asignment 1')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9999)
    parser.add_argument('--storage', type=str, default='dps1/results/windowed_aggregation/')
    parser.add_argument('--master', type=str, required=True,
                        help='the name of the spark master node (example: spark://node102.cm.cluster:7077)')

    args = parser.parse_args()

    os.makedirs(args.storage, exist_ok=True)

    aggregation = WindowedAggregation(args.master, args.host, args.port, args.storage)
    aggregation.run()
