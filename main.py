from benchmarks.spark.WindowedAggregation import WindowedAggregation


if __name__ == '__main__':
    aggregation = WindowedAggregation('localhost', 9017)
    aggregation.run()
