package com.slack.kaldb.logstore.search.aggregations;

public class AvgAggregation implements Aggregation {
  private double sum;
  private long count;

  public AvgAggregation(double sum, long count) {
    this.sum = sum;
    this.count = count;
  }

  @Override
  public void merge(Aggregation objectToMerge) {
    if (!(objectToMerge instanceof AvgAggregation)) {
      throw new IllegalArgumentException("objectToMerge must be of same type [AvgAggregation]");
    } else {
      this.sum += ((AvgAggregation) objectToMerge).sum;
      this.count += ((AvgAggregation) objectToMerge).count;
    }
  }

  @Override
  public Object getMergedResult() {
    return sum / count;
  }
}
