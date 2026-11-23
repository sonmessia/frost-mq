package io.frostmq.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TopicMatcherTest {
    @Test void mqttWildcardRules() {
        assertTrue(TopicMatcher.match("a/+", "a/"));
        assertTrue(TopicMatcher.match("a/#", "a"));
        assertTrue(TopicMatcher.match("a/#", "a/b/c"));
        assertTrue(TopicMatcher.match("+/+", "/"));
        assertFalse(TopicMatcher.match("a/+", "a"));
        assertFalse(TopicMatcher.match("a/+", "a/b/c"));
        assertFalse(TopicMatcher.match("#", "$SYS/uptime"));
        assertFalse(TopicMatcher.match("+/uptime", "$SYS/uptime"));
        assertTrue(TopicMatcher.match("$SYS/#", "$SYS/uptime"));
        assertTrue(TopicMatcher.match("a/+", "a/$SYS"));
        assertFalse(TopicMatcher.match("A/#", "a/b"));
    }

    @Test void invalidTopicsAndFilters() {
        for (String filter : new String[]{"", "a/#/b", "a#", "a/+b", "a/\0", "\ud800"}) assertFalse(TopicMatcher.validFilter(filter));
        for (String topic : new String[]{"", "#", "a/+", "a/\0", "\udc00"}) assertFalse(TopicMatcher.validTopic(topic));
        assertTrue(TopicMatcher.validTopic("/"));
        assertTrue(TopicMatcher.validTopic("frozen/雪"));
    }

    @Test void aclChecksContainmentNotMatchingAWildcardAsALiteral() {
        assertTrue(TopicMatcher.covers("coldchain/#", "coldchain/+/+/telemetry"));
        assertTrue(TopicMatcher.covers("coldchain/+/sensor01/cmd", "coldchain/site/sensor01/cmd"));
        assertFalse(TopicMatcher.covers("coldchain/+/sensor01/cmd", "coldchain/+/+/cmd"));
        assertFalse(TopicMatcher.covers("coldchain/+/sensor01/cmd", "coldchain/#"));
        assertFalse(TopicMatcher.covers("#", "$SYS/#"));
        assertTrue(TopicMatcher.covers("$SYS/#", "$SYS/+"));
    }
}
