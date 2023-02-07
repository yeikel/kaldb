package com.slack.kaldb.logstore.search.queryparser;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class KaldbQueryParserTest {
    @Test
    public void testInit() {
        assertThatIllegalArgumentException().isThrownBy(() -> new KaldbQueryParser("test", new StandardAnalyzer(), null));
        assertThatIllegalArgumentException().isThrownBy(() -> new KaldbQueryParser("test", new StandardAnalyzer(), new ConcurrentHashMap<>()));
    }
}
