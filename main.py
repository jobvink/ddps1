from benchmarks.spark.WindowedAggregation import WindowedAggregation


if __name__ == '__main__':
    aggregation = WindowedAggregation('localhost', 9019)
    aggregation.run()
