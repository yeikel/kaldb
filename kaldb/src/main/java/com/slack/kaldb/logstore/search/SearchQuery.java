package com.slack.kaldb.logstore.search;

import java.util.List;
import java.util.Map;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final String queryStr;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final int howMany;
  public final SearchAggregation searchAggregations;
  public final List<String> chunkIds;

  public SearchQuery(
      String dataset,
      String queryStr,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      SearchAggregation searchAggregations,
      List<String> chunkIds) {
    this.dataset = dataset;
    this.queryStr = queryStr;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.howMany = howMany;
    this.searchAggregations = searchAggregations;
    this.chunkIds = chunkIds;
  }

  @Deprecated
  public SearchQuery(
      String dataset,
      String queryStr,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int bucketCount,
      List<String> chunkIds) {
    this.dataset = dataset;
    this.queryStr = queryStr;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.howMany = howMany;

    if (bucketCount > 0) {
      this.searchAggregations =
          new SearchAggregation(
              "1",
              "date_histogram",
              Map.of(
                  "interval",
                  ((endTimeEpochMs - startTimeEpochMs) / bucketCount) + "S",
                  "extended_bounds",
                  Map.of(
                      "min", startTimeEpochMs,
                      "max", endTimeEpochMs)),
              List.of());
    } else {
      this.searchAggregations = new SearchAggregation();
    }
    this.chunkIds = chunkIds;
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "dataset='"
        + dataset
        + '\''
        + ", queryStr='"
        + queryStr
        + '\''
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", howMany="
        + howMany
        + ", searchAggregations="
        + searchAggregations
        + ", chunkIds="
        + chunkIds
        + '}';
  }
}
