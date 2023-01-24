package com.slack.kaldb.logstore.search.aggregations;

public class BucketAggregation implements Aggregation{

  private final Object key;
  private final Aggregation value;

  public BucketAggregation(Object key, Aggregation value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public void merge(Aggregation objectToMerge) {
    if (!(objectToMerge instanceof BucketAggregation)) {
      throw new IllegalArgumentException(
          "objectToMerge must be of same type [BucketAggregation]");
    } else if (!(this.key.equals(((BucketAggregation) objectToMerge).key))) {
      throw new IllegalArgumentException("objectToMerge must have the same key");
    }

    this.value.merge(((BucketAggregation) objectToMerge).value);
  }

  @Override
  public Object getMergedResult() {
    return value.getMergedResult();
  }
}
