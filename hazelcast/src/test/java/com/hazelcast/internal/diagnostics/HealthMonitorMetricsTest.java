package com.hazelcast.internal.diagnostics;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class HealthMonitorMetricsTest extends AbstractMetricsTest {

    @Override
    Config configure() {
        return new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(),
                ProbeLevel.INFO.name());
    }

    /**
     * In order to get the TCP statistics we need a real {@link ConnectionManager}.
     */
    @Rule
    public final OverridePropertyRule useRealNetwork = set(HAZELCAST_TEST_USE_NETWORK, "true");

    /**
     * Check that the statistics used by the {@link HealthMonitor} are available.
     * 
     * This checks the *corrected* list extracted from {@link HealthMonitor}
     * implementation.
     */
    @Test
    public void healthMonitorMetrics() {
        assertHasAllStatsEventually(
                "client.endpoint.count",
                "cluster.clock.clusterTimeDiff",
                "type=internal-executor instance=hz:async queueSize",
                "type=internal-executor instance=hz:client queueSize",
                "type=internal-executor instance=hz:scheduled queueSize",
                "type=internal-executor instance=hz:system queueSize",
                "type=internal-executor instance=hz:query queueSize",

                "event.eventQueueSize",

                "gc.minorCount",
                "gc.minorTime",
                "gc.majorCount",
                "gc.majorTime",
                "gc.unknownCount",
                "gc.unknownTime",

                "runtime.availableProcessors",
                "runtime.maxMemory",
                "runtime.freeMemory",
                "runtime.totalMemory",
                "runtime.usedMemory",

                "thread.peakThreadCount",
                "thread.threadCount",

                "os.processCpuLoad",
                "os.systemLoadAverage",
                "os.systemCpuLoad",
                "os.totalPhysicalMemorySize",
                "os.freePhysicalMemorySize",
                "os.totalSwapSpaceSize",
                "os.freeSwapSpaceSize",

                "operation.queueSize",
                "operation.priorityQueueSize",
                "operation.responseQueueSize",
                "operation.runningCount",
                "operation.completedCount",
                "operation.invocations.pending",
                "operation.invocations.usedPercentage",

                "proxy.proxyCount",

                "tcp.connection.activeCount",
                "tcp.connection.count",
                "tcp.connection.clientCount");
    }

}
