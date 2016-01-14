package com.hazelcast.internal.monitors;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SingleLinePerformanceLogWriterTest extends HazelcastTestSupport {

    private SingleLinePerformanceLogWriter writer;

    @Before
    public void setup() {
        writer = new SingleLinePerformanceLogWriter();
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

        assertEquals("SomeSection[boolean=true,long=10,SubSection[integer=10],string=foo,double=11.0,foobar]",
                writer.sb.toString());
    }

    @Test
    public void testWrite() {
        writer.write(new PerformanceMonitorPlugin() {
            @Override
            public long getPeriodMillis() {
                return 0;
            }

            @Override
            public void onStart() {

            }

            @Override
            public void run(PerformanceLogWriter writer) {
                writer.startSection("somesection");
                writer.endSection();
            }
        });

        String content = writer.sb.toString();
        String[] split = content.split(" ");
        assertEquals(2, split.length);
        assertEquals("somesection[]" + LINE_SEPARATOR, split[1]);
    }
}
