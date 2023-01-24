package com.slack.kaldb.logstore.search.aggregations;

public interface Mergable {

  void merge(Mergable objectToMerge);

  Object getMergedResult();
}
