/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class HealthMonitorMetricsTest extends HazelcastInstanceMetricsIntegrationTest {

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
