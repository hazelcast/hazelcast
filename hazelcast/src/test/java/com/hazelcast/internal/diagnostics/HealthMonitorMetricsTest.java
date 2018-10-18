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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
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

    private static final String NS = MetricsSource.TAG_NAMESPACE + "=";

    private static final String[] LONG_METRICS = {
            NS + "client.endpoint count",
            NS + "cluster.clock clusterTimeDiff",
            NS + "internal-executor instance=hz:async queueSize",
            NS + "internal-executor instance=hz:client queueSize",
            NS + "internal-executor instance=hz:scheduled queueSize",
            NS + "internal-executor instance=hz:system queueSize",
            NS + "internal-executor instance=hz:query queueSize",

            NS + "event eventQueueSize",

            NS + "gc minorCount",
            NS + "gc minorTime",
            NS + "gc majorCount",
            NS + "gc majorTime",
            NS + "gc unknownCount",
            NS + "gc unknownTime",

            NS + "runtime availableProcessors",
            NS + "runtime maxMemory",
            NS + "runtime freeMemory",
            NS + "runtime totalMemory",
            NS + "runtime usedMemory",

            NS + "thread peakThreadCount",
            NS + "thread threadCount",

            NS + "os totalPhysicalMemorySize",
            NS + "os freePhysicalMemorySize",
            NS + "os totalSwapSpaceSize",
            NS + "os freeSwapSpaceSize",

            NS + "operation queueSize",
            NS + "operation priorityQueueSize",
            NS + "operation runningCount",
            NS + "operation completedCount",
            NS + "operation.responses responseQueueSize",
            NS + "operation.invocations pending",

            NS + "proxy proxyCount"
    };

    private static final String[] DOUBLE_METRICS = {
            NS + "operation.invocations usedPercentage",
            NS + "os processCpuLoad",
            NS + "os systemLoadAverage",
            NS + "os systemCpuLoad",
    };

    /**
     * Check that the statistics used by the {@link HealthMonitor} are generally available.
     *
     * This checks the *corrected* list extracted from {@link HealthMonitor} implementation.
     */
    @Test
    public void healthMonitorMetrics() {
        setProbeLevel(ProbeLevel.MANDATORY);

        assertEventuallyHasAllStats(LONG_METRICS);
        assertEventuallyHasAllStats(DOUBLE_METRICS);
    }

    /**
     * Check that the statistics used by the {@link HealthMonitor} are available as gauges.
     */
    @Test
    public void healthMonitorMetricsAsGauge() {
        setProbeLevel(ProbeLevel.MANDATORY);

        LongGauge[] longGauges = new LongGauge[LONG_METRICS.length];
        for (int i = 0; i < LONG_METRICS.length; i++) {
            longGauges[i] = registry.newLongGauge(LONG_METRICS[i]);
        }
        DoubleGauge[] doubleGauges = new DoubleGauge[DOUBLE_METRICS.length];
        for (int i = 0; i < DOUBLE_METRICS.length; i++) {
            doubleGauges[i] = registry.newDoubleGauge(DOUBLE_METRICS[i]);
        }
        assertHasStats(1, "os"); // force a collection to connect gauges

        assertConnected(longGauges, LONG_METRICS);
        assertConnected(doubleGauges, DOUBLE_METRICS);
    }

    private void assertConnected(Object[] gauges, String[] names) {
        for (int i = 0; i < gauges.length; i++) {
            assertConnected(gauges[i], names[i]);
        }
    }

    private void assertConnected(Object gauge, String name) {
        try {
            Field field = gauge.getClass().getDeclaredField("gauge");
            field.setAccessible(true);
            assertNotNull("Gauge `" + name + "` not connected.", field.get(gauge));
        } catch (Exception e) {
            fail("Test setup failure: " + e.getMessage());
        }
    }
}
