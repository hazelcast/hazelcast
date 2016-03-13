package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_METRICS_PERIOD_SECONDS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MetricsPluginTest extends AbstractPerformanceMonitorPluginTest {

    private MetricsPlugin plugin;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PERFORMANCE_MONITOR_ENABLED, "true");
        config.setProperty(PERFORMANCE_MONITOR_METRICS_PERIOD_SECONDS, "1");
        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        metricsRegistry = nodeEngineImpl.getMetricsRegistry();
        plugin = new MetricsPlugin(nodeEngineImpl);
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        long periodMillis = plugin.getPeriodMillis();
        assertEquals(SECONDS.toMillis(1), periodMillis);
    }

    @Test
    public void testRunWithProblematicProbe() throws Throwable {
        metricsRegistry.register(this, "broken", MANDATORY, new LongProbeFunction() {
            @Override
            public long get(Object source) throws Exception {
                throw new RuntimeException("error");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                logWriter.clean();
                plugin.run(logWriter);
                assertContains("broken=java.lang.RuntimeException:error");
            }
        });
    }

    @Test
    public void testRun() throws IOException {
        plugin.run(logWriter);

        // we just test a few to make sure the metrics are written.
        assertContains("client.endpoint.count=0");
        assertContains("operation.response-queue.size=0");
    }
}
