package com.slack.kaldb.logstore.search.aggregations;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.ResponseBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

// todo (Wed)
//  [x] implement the count agg createFacetMerger
//  [x] implement the avg agg
//  [x] implement basic ability in logImpl to switch between these two aggs
//  [] test implementation

// todo (Thurs)
//  [] Start porting proper facets

// a date histogram correlates to a FacetRange
// the "facetRange" is what has the mergables on it
// you can "merge" two FacetRange results together

// FacetProcessor is the "wrapper" to build the associated Facets (ie FacetRange, which is a
// FacetRequest)
// has a createAccs() methods
// has "collect" methods
// has "setNextReader" methods

// FacetRequest is like a DateHistogram, or a Terms (infinitely nestable sub-facets)

public class DateHistogramAggregation extends SimpleCollector implements Mergable {

  private final long startEpochMs;
  private final long endEpochMs;
  private final long bucketCount;

  private final SlotAcc slotAcc;

  private final ValueSource valueSource;

  public DateHistogramAggregation(
      long startEpochMs, long endEpochMs, long bucketCount, SlotAcc slotAcc) {
    this.startEpochMs = startEpochMs;
    this.endEpochMs = endEpochMs;
    this.bucketCount = bucketCount;
    this.slotAcc = slotAcc;
    this.valueSource = new LongFieldSource(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
  }

  private int getBucket(long epochMs) {
    // todo - better math
    long bucketSize =
        Double.valueOf(Math.floor((endEpochMs - startEpochMs) / bucketCount)).longValue();
    int index = Double.valueOf(Math.ceil((epochMs - startEpochMs) / bucketSize)).intValue();
    return Math.max(index - 1, 0);
  }

  @Override
  public void merge(Mergable objectToMerge) {
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
      try {
        for (int i = 0; i < bucketCount; i++) {
          // public class CountAgg extends SimpleAggValueSource
          AggValueSource slotValue = (AggValueSource) this.slotAcc.getValue(i);
          FacetMerger merger = slotValue.createFacetMerger(null);
          merger.merge(((DateHistogramAggregation) objectToMerge).slotAcc.getValue(i), null);
        }
      } catch (Exception e) {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public List<ResponseBucket> getMergedResult() {
    try {
      List<ResponseBucket> foo = new ArrayList<>();
      for (int i = 0; i < bucketCount; i++) {
        long bucketSize =
            Double.valueOf(Math.floor((endEpochMs - startEpochMs) / bucketCount)).longValue();
        long getKey = new Double(startEpochMs + (bucketSize * i) + bucketSize / 2).longValue();
        Object value = this.slotAcc.getValue(i);

        // todo
        if (this.slotAcc instanceof SlotAcc.CountSlotArrAcc) {
          foo.add(new ResponseBucket(List.of(getKey), (Long) value, Map.of()));
        } else if (this.slotAcc instanceof SlotAcc.AvgSlotAcc) {
          // todo - what do we do about the doc count here?
          foo.add(new ResponseBucket(List.of(getKey), 0, Map.of("value", value)));
        } else {
          throw new IllegalArgumentException();
        }

//        // (Long) value,
//
//        long val = 0L;
//        try {
//          val = (Long) value;
//        } catch (Exception e) {
//          // ignored
//        }
//
//        foo.add(new ResponseBucket(List.of(getKey), val, Map.of("value", value)));
      }
      return foo;
    } catch (Exception e) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  protected void doSetNextReader(final LeafReaderContext context) throws IOException {
    slotAcc.setNextReader(context);
  }

  @Override
  public void collect(int doc) throws IOException {
    if (slotAcc != null) {
      FunctionValues values = valueSource.getValues(Map.of(), slotAcc.currentReaderContext);
      long timestamp = values.longVal(doc);
      int slot = getBucket(timestamp);

      slotAcc.collect(doc, slot, null);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
