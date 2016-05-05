package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static aQute.bnd.maven.Pom.Scope.test;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SingleLineDiagnosticsLogWriterTest extends HazelcastTestSupport {

    private SingleLineDiagnosticsLogWriter writer;

    @Before
    public void setup() {
        writer = new SingleLineDiagnosticsLogWriter();
    }

    @Test
    public void test() {
        writer.startSection("SomeSection");

        writer.writeKeyValueEntry("boolean", true);
        writer.writeKeyValueEntry("long", 10l);

        writer.startSection("SubSection");
        writer.writeKeyValueEntry("integer", 10);
        writer.endSection();

        writer.writeKeyValueEntry("string", "foo");
        writer.writeKeyValueEntry("double", 11d);
        writer.writeEntry("foobar");

        writer.endSection();

        assertTrue(writer.sb.toString().contains("SomeSection[boolean=true,long=10,SubSection[integer=10],string=foo,double=11.0,foobar]"));
    }

    @Test
    public void testWrite() {
        DummyDiagnosticsPlugin plugin = new DummyDiagnosticsPlugin();
        plugin.run(writer);

        String content = writer.sb.toString();
        String[] split = content.split(" ");
        assertEquals(3, split.length);
        assertEquals("somesection[]" + LINE_SEPARATOR, split[2]);
    }

    private static class DummyDiagnosticsPlugin extends DiagnosticsPlugin {
        DummyDiagnosticsPlugin() {
            super(Logger.getLogger(SingleLineDiagnosticsLogWriterTest.class));
        }

        @Override
        public long getPeriodMillis() {
            return 0;
        }

        @Override
        public void onStart() {

        }

        @Override
        public void run(DiagnosticsLogWriter writer) {
            writer.startSection("somesection");
            writer.endSection();
        }
    }
}
