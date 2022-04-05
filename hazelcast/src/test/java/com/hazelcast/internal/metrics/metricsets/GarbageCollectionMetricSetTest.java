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

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GarbageCollectionMetricSetTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;
    private GarbageCollectionMetricSet.GcStats gcStats;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        GarbageCollectionMetricSet.register(metricsRegistry);
        gcStats = new GarbageCollectionMetricSet.GcStats();
    }

    @After
    public void tearDown() {
        metricsRegistry.shutdown();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(GarbageCollectionMetricSet.class);
    }

    @Test
    public void minorCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.minorCount");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.minorCount, gauge.read(), 1);
        });
    }

    @Test
    public void minorTime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.minorTime");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.minorTime, gauge.read(), SECONDS.toMillis(1));
        });
    }

    @Test
    public void majorCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.majorCount");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.majorCount, gauge.read(), 1);
        });
    }

    @Test
    public void majorTime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.majorTime");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.majorTime, gauge.read(), SECONDS.toMillis(1));
        });
    }


    @Test
    public void unknownCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.unknownCount");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.unknownCount, gauge.read(), 1);
        });
    }

    @Test
    public void unknownTime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("gc.unknownTime");
        assertTrueEventually(() -> {
            gcStats.run();
            assertEquals(gcStats.unknownTime, gauge.read(), SECONDS.toMillis(1));
        });
    }
}
