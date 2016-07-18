package com.hazelcast.internal.diagnostics;

import com.hazelcast.test.HazelcastTestSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractDiagnosticsPluginTest extends HazelcastTestSupport {

    protected DiagnosticsLogWriter logWriter = new MultiLineDiagnosticsLogWriter();

    protected void clean() {
        logWriter.clean();
    }

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
