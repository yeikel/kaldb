package com.slack.kaldb.logstore.search.aggregations;

public class CountAggregation implements Aggregation {
  protected long count;

  public CountAggregation(long count) {
    this.count = count;
  }

  @Override
  public void merge(Aggregation objectToMerge) {
    if (!(objectToMerge instanceof CountAggregation)) {
      throw new IllegalArgumentException("objectToMerge must be of same type [CountAggregation]");
    } else {
      this.count += ((CountAggregation) objectToMerge).count;
    }
  }

  @Override
  public Object getMergedResult() {
    return count;
  }
}
