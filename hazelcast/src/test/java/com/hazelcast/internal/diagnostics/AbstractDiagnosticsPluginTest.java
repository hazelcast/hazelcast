package com.hazelcast.internal.diagnostics;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.io.CharArrayWriter;
import java.io.PrintWriter;

public class AbstractDiagnosticsPluginTest extends HazelcastTestSupport {

    protected DiagnosticsLogWriter logWriter;
    private CharArrayWriter out;

    @Before
    public void setupLogWriter(){
        logWriter = new MultiLineDiagnosticsLogWriter();
        out = new CharArrayWriter();
        logWriter.init(new PrintWriter(out));
    }

    protected void reset(){
        out.reset();
    }

    protected String getContent() {
        return out.toString();
    }

    protected void assertContains(String expected) {
        assertContains(getContent(), expected);
    }

    protected void assertNotContains(String expected) {
        assertNotContains(getContent(), expected);
    }
}
