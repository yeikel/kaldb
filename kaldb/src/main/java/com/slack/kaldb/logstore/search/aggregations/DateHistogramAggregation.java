package com.slack.kaldb.logstore.search.aggregations;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DateHistogramAggregation implements Aggregation {

  private final long startEpochMs;
  private final long endEpochMs;
  private final long bucketCount;

  private final List<BucketAggregation> bucketList = new ArrayList<>();

  public DateHistogramAggregation(long startEpochMs, long endEpochMs, int bucketCount) {
    this.startEpochMs = startEpochMs;
    this.endEpochMs = endEpochMs;
    this.bucketCount = bucketCount;

    long step = (endEpochMs - startEpochMs) / bucketCount;
    for (long i = startEpochMs; i < endEpochMs; i += step) {
      // this bucket is keyed against the midpoint
      bucketList.add(new BucketAggregation((i + i + step) / 2, null));
    }
  }

  public DateHistogramAggregation(
      long startEpochMs, long endEpochMs, List<Aggregation> bucketList) {
    this.startEpochMs = startEpochMs;
    this.endEpochMs = endEpochMs;
    this.bucketCount = bucketList.size();

    long step = (endEpochMs - startEpochMs) / bucketCount;
    int counter = 0;
    for (long i = startEpochMs; i < endEpochMs; i += step) {
      this.bucketList.add(new BucketAggregation((i + i + step) / 2, bucketList.get(counter)));
      counter++;
    }
  }

  @Override
  public void merge(Aggregation objectToMerge) {
    // do the merge thing
    if (!(objectToMerge instanceof DateHistogramAggregation)) {
      throw new IllegalArgumentException(
          "objectToMerge must be of same type [DateHistogramAggregation]");
    } else if ((this.startEpochMs != ((DateHistogramAggregation) objectToMerge).startEpochMs)
        || (this.endEpochMs != ((DateHistogramAggregation) objectToMerge).endEpochMs)) {
      throw new IllegalArgumentException("objectToMerge must have same start end end epoch");
    } else if (this.bucketCount != ((DateHistogramAggregation) objectToMerge).bucketCount) {
      throw new IllegalArgumentException("objectToMerge must have the same bucketSize");
    } else {
      for (int i = 0; i < this.bucketList.size(); i++) {
        this.bucketList.get(i).merge(((DateHistogramAggregation) objectToMerge).bucketList.get(i));
      }
    }
  }

  @Override
  public Object getMergedResult() {
    return bucketList.stream().map(BucketAggregation::getMergedResult).collect(Collectors.toList());
  }
}
