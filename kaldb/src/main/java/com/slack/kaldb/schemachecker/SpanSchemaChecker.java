package com.slack.kaldb.schemachecker;

import com.slack.service.murron.trace.Trace;

import java.util.List;

/**
 * This class consumes the schema checker schema and builds a schema checker. It also provides a
 * checkSpan method to check a span against a schema.
 */
public class SpanSchemaChecker {
  private com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.SpanSchemaCheckerConfig
      spanSchemaCheckerConfig;

  public boolean checkSpan(Trace.Span span) {
    for (com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.Rule rule :
        spanSchemaCheckerConfig.getRulesList()) {
      matchesRule(span, rule);
    }
      return false;
  }

  public boolean matchesRule(Trace.Span span, com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.Rule rule) {
    return matchesTagRules(rule.getTagRulesList(), span) && matchesServiceNameRegExes(rule.getServiceNameRegExesList(), span);
  }

  private boolean matchesTagRules(List<com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.TagRule> tagRulesList, Trace.Span span) {
    for (com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.TagRule tagRule: tagRulesList) {
      matchTagRule(tagRule, span);
    }
  }

  private boolean matchTagRule(com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.TagRule tagRule, Trace.Span span) {
    if (!tagRule.getName().isEmpty() && span.getName() == tagRule.getName()) {
      return false;
    }
    // service name matches.
  }

  private String getServiceName(Trace.Span span) {

  }

  private boolean matchesServiceNameRegExes(List<com.slack.kaldb.proto.spanschemaconfig.SpanSchemaChecker.ServiceNameRegEx> serviceNameRegExesList, Trace.Span span) {
  }
}
