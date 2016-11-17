package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StoreLatencyPluginResetTest extends AbstractDiagnosticsPluginTest {

    @Test
    public void test() throws InterruptedException {
        Properties props = new Properties();
        props.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "1");
        props.put(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(), "2");

        HazelcastProperties properties = new HazelcastProperties(props);
        StoreLatencyPlugin plugin = new StoreLatencyPlugin(Logger.getLogger(StoreLatencyPlugin.class), properties);

        StoreLatencyPlugin.LatencyProbe probe = plugin.newProbe("foo", "queue", "somemethod");

        probe.recordValue(MICROSECONDS.toNanos(1));
        probe.recordValue(MICROSECONDS.toNanos(2));
        probe.recordValue(MICROSECONDS.toNanos(5));

        // run for the first time.
        plugin.run(logWriter);
        assertContains("max(us)=5");

        // reset the logWriter so we don't get previous run content.
        reset();
        // run for the second time;
        plugin.run(logWriter);
        // now it should still contain the old statistics
        assertContains("max(us)=5");

        // reset the logWriter so we don't get previous run content.
        reset();
        // run for the third time; now the stats should be gone
        plugin.run(logWriter);
        // now it should not contain the old statistics
        assertNotContains("max(us)=5");
    }

}
