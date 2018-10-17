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

import com.hazelcast.internal.metrics.MetricsSource;
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
        String ns = MetricsSource.TAG_NAMESPACE + "=";
        assertEventuallyHasAllStats(
                ns + "client.endpoint count",
                ns + "cluster.clock clusterTimeDiff",
                ns + "internal-executor instance=hz:async queueSize",
                ns + "internal-executor instance=hz:client queueSize",
                ns + "internal-executor instance=hz:scheduled queueSize",
                ns + "internal-executor instance=hz:system queueSize",
                ns + "internal-executor instance=hz:query queueSize",

                ns + "event eventQueueSize",

                ns + "gc minorCount",
                ns + "gc minorTime",
                ns + "gc majorCount",
                ns + "gc majorTime",
                ns + "gc unknownCount",
                ns + "gc unknownTime",

                ns + "runtime availableProcessors",
                ns + "runtime maxMemory",
                ns + "runtime freeMemory",
                ns + "runtime totalMemory",
                ns + "runtime usedMemory",

                ns + "thread peakThreadCount",
                ns + "thread threadCount",

                ns + "os processCpuLoad",
                ns + "os systemLoadAverage",
                ns + "os systemCpuLoad",
                ns + "os totalPhysicalMemorySize",
                ns + "os freePhysicalMemorySize",
                ns + "os totalSwapSpaceSize",
                ns + "os freeSwapSpaceSize",

                ns + "operation queueSize",
                ns + "operation priorityQueueSize",
                ns + "operation runningCount",
                ns + "operation completedCount",
                ns + "operation.responses responseQueueSize",
                ns + "operation.invocations pending",
                ns + "operation.invocations usedPercentage",

                ns + "proxy proxyCount");
    }

}
