package com.hazelcast.internal.diagnostics;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.io.CharArrayWriter;
import java.io.PrintWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractDiagnosticsPluginTest extends HazelcastTestSupport {

    protected DiagnosticsLogWriter logWriter;
    private CharArrayWriter out;

    @Before
    public void setupLogWriter(){
        logWriter = new MultiLineDiagnosticsLogWriter();
        out = new CharArrayWriter();
        logWriter.init(new PrintWriter(out));
    }

    protected String getContent() {
        return out.toString();
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
