package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationTest {
  private static final Logger LOG = LoggerFactory.getLogger(AggregationTest.class);

  @Test
  public void test() {

    AvgAggregation avgAggregation = new AvgAggregation(100, 10); // avg should be "10"
    AvgAggregation avgAggregationToAdd = new AvgAggregation(100, 1); // avg should be "100"

    avgAggregation.merge(avgAggregationToAdd); // new avg is 18.18 (single value)
    LOG.info("avgAggregation - {}", avgAggregation.getMergedResult());

    CountAggregation countAggregation = new CountAggregation(100);
    CountAggregation countAggregationToAdd = new CountAggregation(10);

    countAggregation.merge(countAggregationToAdd); // new count is 110 (single value)
    LOG.info("countAggregation - {}", countAggregation.getMergedResult());

    DateHistogramAggregation dateHistogramAggregation =
        new DateHistogramAggregation(
            1000, 2000, List.of(new CountAggregation(10), new CountAggregation(10)));

    DateHistogramAggregation dateHistogramAggregationToAdd =
        new DateHistogramAggregation(
            1000, 2000, List.of(new CountAggregation(20), new CountAggregation(40)));
    dateHistogramAggregation.merge(dateHistogramAggregationToAdd); // list<Aggregation>  of counts - bucket1 = 30, bucket2 = 50
    LOG.info("dateHistogramAggregation - {}", dateHistogramAggregation.getMergedResult());

    DateHistogramAggregation dateHistogramAggregationAvg = new DateHistogramAggregation(
        1000, 2000, List.of(
            new AvgAggregation(100, 10),
            new AvgAggregation(100, 1)
    ));
    DateHistogramAggregation dateHistogramAggregationAvgToAdd = new DateHistogramAggregation(
        1000, 2000, List.of(
            new AvgAggregation(100, 10),
          new AvgAggregation(200, 1)
    ));
    dateHistogramAggregationAvg.merge(dateHistogramAggregationAvgToAdd); // list<Aggregation> of avgs - bucket1 = 10 (avg), bucket2 = 150 (avg)
    LOG.info("dateHistogramAggregationAvg - {}", dateHistogramAggregationAvg.getMergedResult());

    TermsAggregation termsAggregation = new TermsAggregation(Map.of("term1", new CountAggregation(10),
        "term2", new CountAggregation(20)));
    TermsAggregation termsAggregationToAdd = new TermsAggregation(Map.of("term3", new CountAggregation(5),
        "term1", new CountAggregation(40)));

    termsAggregation.merge(termsAggregationToAdd);
    LOG.info("termsAggregation - {}", termsAggregation.getMergedResult());

  }

  @Test
  public void nestedTerms() {
    TermsAggregation innerTermsOne = new TermsAggregation(Map.of("innerTerm1", new CountAggregation(10),
        "innerTerm2", new CountAggregation(20)));
    TermsAggregation innerTermsTwo = new TermsAggregation(Map.of("innerTerm3", new CountAggregation(5),
        "innerTerm1", new CountAggregation(40)));
    TermsAggregation innerTermsThree = new TermsAggregation(Map.of("innerTerm1", new CountAggregation(5)));

    TermsAggregation outerTermsOne = new TermsAggregation(Map.of("outerTerms1", innerTermsOne));
    TermsAggregation outerTermsTwo = new TermsAggregation(Map.of("outerTerms2", innerTermsTwo));
    TermsAggregation outerTermsThree = new TermsAggregation(Map.of("outerTerms1",innerTermsThree));

    outerTermsOne.merge(outerTermsTwo);
    LOG.info("outerTermsOne - {}", outerTermsOne.getMergedResult());

    outerTermsOne.merge(outerTermsThree);
    LOG.info("outerTermsOne(3) - {}", outerTermsOne.getMergedResult());
  }
}
