package com.slack.kaldb.schemachecker;

import com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker;
import com.slack.service.murron.trace.Trace;
import org.junit.Test;

public class SchemaCheckerTest {

    @Test
    public void testMatchTagRule() {
        Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();
        spanBuilder.setDurationMicros(100)
                .setStartTimestampMicros(1000000)
                .setName("testEvent")
                .setTags(0, Trace.KeyValue.newBuilder().setKey("testKey").setVTypeValue(Trace.ValueType.STRING_VALUE).setVStr("test"))
                .build();

        SpanSchemaChecker.SpanSchemaCheckerConfig.Builder schemaConfigBuilder = SpanSchemaChecker.SpanSchemaCheckerConfig.newBuilder();
        SpanSchemaChecker.TagRule tagRule = SpanSchemaChecker.TagRule.newBuilder().build();
        schemaConfigBuilder.addRules(0, SpanSchemaChecker.Rule.newBuilder().setTagRules(0, tagRule)).build();
    }
}
