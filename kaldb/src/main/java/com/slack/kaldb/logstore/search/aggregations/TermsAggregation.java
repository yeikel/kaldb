package com.slack.kaldb.logstore.search.aggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TermsAggregation implements Aggregation {
  private final Map<Object, Aggregation> terms;

  public TermsAggregation(Map<Object, Aggregation> terms) {
    this.terms = new HashMap<>(terms);
  }

  @Override
  public void merge(Aggregation objectToMerge) {
    if (!(objectToMerge instanceof TermsAggregation)) {
      throw new IllegalArgumentException("objectToMerge must be of same type [TermsAggregation]");
    } else {
      ((TermsAggregation) objectToMerge).terms.forEach((key, value) -> {
        if (this.terms.containsKey(key)) {
          this.terms.get(key).merge(value);
        } else {
          this.terms.put(key, value);
        }
      });
    }
  }

  @Override
  public Object getMergedResult() {
    Map<Object, Object> merged = new HashMap<>();

    terms.forEach((key, value) -> {
      merged.put(key, value.getMergedResult());
    });

    return merged;
  }
}
