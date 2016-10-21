package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbeImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StoreLatencyPluginTest extends AbstractDiagnosticsPluginTest {

    private StoreLatencyPlugin plugin;

    @Before
    public void setup() {
        HazelcastProperties properties = new HazelcastProperties(new Properties());
        plugin = new StoreLatencyPlugin(Logger.getLogger(StoreLatencyPlugin.class), properties);
    }

    @Test
    public void getProbe() {
        LatencyProbe probe = plugin.newProbe("foo", "queue", "somemethod");
        assertNotNull(probe);
    }

    @Test
    public void getProbe_whenSameProbeRequestedMoreThanOnce() {
        LatencyProbe probe1 = plugin.newProbe("foo", "queue", "somemethod");
        LatencyProbe probe2 = plugin.newProbe("foo", "queue", "somemethod");
        assertSame(probe1, probe2);
    }

    @Test
    public void testMaxMicros() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(1000));
        probe.recordValue(MICROSECONDS.toNanos(4));

        assertEquals(1000, probe.maxMicros);
    }

    @Test
    public void testCount() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(10));

        assertEquals(3, probe.count);
    }

    @Test
    public void testTotalMicros() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(20));
        probe.recordValue(MICROSECONDS.toNanos(30));

        assertEquals(60, probe.totalMicros);
    }

    @Test
    public void render() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(100));
        probe.recordValue(MICROSECONDS.toNanos(200));
        probe.recordValue(MICROSECONDS.toNanos(300));

        plugin.run(logWriter);

        assertContains("foo");
        assertContains("queue");
        assertContains("somemethod");
        assertContains("count=3");
        assertContains("totalTime(us)=600");
        assertContains("avg(us)=200");
        assertContains("max(us)=300");
        assertContains("100..199us=1");
        assertContains("200..399us=2");
    }
}
