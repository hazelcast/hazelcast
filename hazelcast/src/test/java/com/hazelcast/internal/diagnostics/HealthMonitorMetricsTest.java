package com.hazelcast.internal.diagnostics;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * @see NetworkMetricsTest#healthMonitorMetrics() 
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class HealthMonitorMetricsTest extends DefaultMetricsTest {

    /**
     * Check that the statistics used by the {@link HealthMonitor} are available.
     * 
     * This checks the *corrected* list extracted from {@link HealthMonitor}
     * implementation.
     */
    @Test
    public void healthMonitorMetrics() {
        setProbeLevel(ProbeLevel.MANDATORY);
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

                "proxy.proxyCount");
    }

}
