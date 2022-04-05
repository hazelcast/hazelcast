/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeMetricSetTest extends HazelcastTestSupport {

    private static final int TEN_MB = 10 * 1024 * 1024;

    private MetricsRegistryImpl metricsRegistry;
    private Runtime runtime;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        RuntimeMetricSet.register(metricsRegistry);
        runtime = Runtime.getRuntime();
    }

    @After
    public void tearDown() {
        metricsRegistry.shutdown();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(RuntimeMetricSet.class);
    }

    @Test
    public void freeMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.freeMemory");

        assertTrueEventually(() -> assertEquals(runtime.freeMemory(), gauge.read(), TEN_MB));
    }

    @Test
    public void totalMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.totalMemory");
        assertTrueEventually(() -> assertEquals(runtime.totalMemory(), gauge.read(), TEN_MB));
    }

    @Test
    public void maxMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.maxMemory");

        assertTrueEventually(() -> assertEquals(runtime.maxMemory(), gauge.read(), TEN_MB));
    }

    @Test
    public void usedMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.usedMemory");

        assertTrueEventually(() -> {
            double expected = runtime.totalMemory() - runtime.freeMemory();
            assertEquals(expected, gauge.read(), TEN_MB);
        });
    }

    @Test
    public void availableProcessors() {
        LongGauge gauge = metricsRegistry.newLongGauge("runtime.availableProcessors");
        assertEquals(runtime.availableProcessors(), gauge.read());
    }

    @Test
    public void uptime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.uptime");
        assertTrueEventually(() -> {
            double expected = ManagementFactory.getRuntimeMXBean().getUptime();
            assertEquals(expected, gauge.read(), TimeUnit.MINUTES.toMillis(1));
        });
    }
}
