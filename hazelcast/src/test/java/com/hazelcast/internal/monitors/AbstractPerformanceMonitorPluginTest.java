package com.hazelcast.internal.monitors;

import com.hazelcast.test.HazelcastTestSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractPerformanceMonitorPluginTest extends HazelcastTestSupport {

    protected PerformanceLogWriter logWriter = new MultiLinePerformanceLogWriter();

    protected String getContent() {
        return logWriter.sb.toString();
    }

    protected void assertContains(String expected) {
        String message = getContent();
        assertTrue("'" + message + "' doesn't contains '" + expected + "'", message.contains(expected));
    }

    protected void assertNotContains(String expected) {
        String message = getContent();
        assertFalse("'" + message + "' doesn't contains '" + expected + "'", message.contains(expected));
    }
}
