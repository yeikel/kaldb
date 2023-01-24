package com.slack.kaldb.logstore.search.aggregations;

public interface Aggregation {

  void merge(Aggregation objectToMerge);

  Object getMergedResult();
}
