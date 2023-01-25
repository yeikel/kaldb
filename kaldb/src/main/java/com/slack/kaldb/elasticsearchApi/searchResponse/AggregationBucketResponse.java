package com.slack.kaldb.elasticsearchApi.searchResponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AggregationBucketResponse {
  @JsonProperty("key")
  private final Object key;

  @JsonProperty("doc_count")
  private final double docCount;

  private Map<String, Map<String, Object>> nested = new HashMap<>();

  @JsonValue
  private Map<String, Object> customSerialize() {
    Map<String, Object> resultMap = new HashMap<>();

    resultMap.put("key", key);
    resultMap.put("key_as_string", String.valueOf(key));
    resultMap.put("doc_count", docCount);
    resultMap.putAll(nested);

    return resultMap;
  }

  public AggregationBucketResponse(Object key, double docCount, Map<String, Map<String, Object>> nested) {
    this.key = key;
    this.docCount = docCount;
    this.nested = nested;
  }

  @JsonProperty("key_as_string")
  public String getKeyAsString() {
    return String.valueOf(key);
  }
}
