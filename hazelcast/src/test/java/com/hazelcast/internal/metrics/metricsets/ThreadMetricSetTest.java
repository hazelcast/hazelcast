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
import java.lang.management.ThreadMXBean;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ThreadMetricSetTest extends HazelcastTestSupport {

    private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        ThreadMetricSet.register(metricsRegistry);
    }

    @After
    public void tearDown() {
        metricsRegistry.shutdown();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(ThreadMetricSet.class);
    }

    @Test
    public void threadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.threadCount");

        assertTrueEventually(() -> assertEquals(MX_BEAN.getThreadCount(), gauge.read(), 10));
    }

    @Test
    public void peakThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.peakThreadCount");

        assertTrueEventually(() -> assertEquals(MX_BEAN.getPeakThreadCount(), gauge.read(), 10));
    }

    @Test
    public void daemonThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.daemonThreadCount");

        assertTrueEventually(() -> assertEquals(MX_BEAN.getDaemonThreadCount(), gauge.read(), 10));
    }

    @Test
    public void totalStartedThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.totalStartedThreadCount");

        assertTrueEventually(() -> assertEquals(MX_BEAN.getTotalStartedThreadCount(), gauge.read(), 10));
    }
}
