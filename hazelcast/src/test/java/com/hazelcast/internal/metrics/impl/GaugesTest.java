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

package com.hazelcast.internal.metrics.impl;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests the {@link MetricsRegistry#newLongGauge(String)} and
 * {@link MetricsRegistry#newDoubleGauge(String)} features.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GaugesTest {

    public static final class AnnotatedObject implements MetricsSource {

        @Probe
        long longFieldGauge = 1;

        @Probe
        double doubleFieldGauge = 2;

        @Probe
        LongProbeFunction longProbeFunctionFieldGauge = new DummyLongProbeFunction(3);

        @Probe
        DoubleProbeFunction doubleProbeFunctionFieldGauge = new DummyDoubleProbeFunction(4d);

        @Probe
        long longMethodGauge() {
            return 5;
        }

        @Probe
        double doubleMethodGauge() {
            return 6d;
        }

        @Probe
        LongProbeFunction longProbeFunctionMethodGauge() {
            return new DummyLongProbeFunction(7);
        }

        @Probe
        DoubleProbeFunction doubleProbeFunctionMethodGauge() {
            return new DummyDoubleProbeFunction(8d);
        }

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.collect(INFO, "directLongProbeFunctionGauge", new DummyLongProbeFunction(9));
            cycle.collect(INFO, "directDoubleProbeFunctionGauge", new DummyDoubleProbeFunction(10d));
        }
    }

    private static final class DummyLongProbeFunction implements LongProbeFunction {

        final long val;

        DummyLongProbeFunction(long val) {
            this.val = val;
        }

        @Override
        public long getAsLong() {
            return val;
        }
    }

    private static final class DummyDoubleProbeFunction implements DoubleProbeFunction {

        final double val;

        DummyDoubleProbeFunction(double val) {
            this.val = val;
        }

        @Override
        public double getAsDouble() {
            return val;
        }
    }

    private MetricsRegistry registry;
    private CollectionContext context;

    @Before
    public void setUp() {
        registry = new MetricsRegistryImpl();
        registry.register(new AnnotatedObject());
        context = registry.openContext(INFO);
    }

    @Test
    public void longFieldGauge() {
        assertLongGauge("longFieldGauge", 1L);
    }

    @Test
    public void doubleFieldGauge() {
        assertDoubleGauge("doubleFieldGauge", 2d);
    }

    @Test
    public void longProbeFunctionFieldGauge() {
        assertLongGauge("longProbeFunctionFieldGauge", 3L);
    }

    @Test
    public void doubleProbeFunctionFieldGauge() {
        assertDoubleGauge("doubleProbeFunctionFieldGauge", 4d);
    }

    @Test
    public void longMethodGauge() {
        assertLongGauge("longMethodGauge", 5L);
    }

    @Test
    public void doubleMethodGauge() {
        assertDoubleGauge("doubleMethodGauge", 6d);
    }

    @Test
    public void longProbeFunctionMethodGauge() {
        assertLongGauge("longProbeFunctionMethodGauge", 7L);
    }

    @Test
    public void doubleProbeFunctionMethodGauge() {
        assertDoubleGauge("doubleProbeFunctionMethodGauge", 8d);
    }

    @Test
    public void directLongProbeFunctionGauge() {
        assertLongGauge("directLongProbeFunctionGauge", 9L);
    }

    @Test
    public void directDoubleProbeFunctionGauge() {
        assertDoubleGauge("directDoubleProbeFunctionGauge", 10d);
    }

    private void assertLongGauge(String name, long expectedValue) {
        LongGauge gauge = registry.newLongGauge(name);
        assertUnconnected(gauge);
        collectMetrics();
        assertConnected(gauge);
        assertEquals(expectedValue, gauge.read());
    }

    private void assertDoubleGauge(String name, double expectedValue) {
        DoubleGauge gauge = registry.newDoubleGauge(name);
        assertUnconnected(gauge);
        collectMetrics();
        assertConnected(gauge);
        assertEquals(expectedValue, gauge.read(), 0.01d);
    }

    private static void assertUnconnected(LongGauge gauge) {
        assertEquals(0L, gauge.read());
        assertEquals("ConnectingLongGauge", gauge.getClass().getSimpleName());
        assertNull(readGaugeField(gauge));
    }

    private static void assertUnconnected(DoubleGauge gauge) {
        assertEquals(0d, gauge.read(), 0.01d);
        assertEquals("ConnectingDoubleGauge", gauge.getClass().getSimpleName());
        assertNull("Gauge was already connected", readGaugeField(gauge));
    }

    private static void assertConnected(Object gauge) {
        assertNotNull("Gauge was not connected", readGaugeField(gauge));
    }

    private static Object readGaugeField(Object gauge) {
        try {
            Field field = gauge.getClass().getDeclaredField("gauge");
            field.setAccessible(true);
            return field.get(gauge);
        } catch (Exception e) {
            fail("Test setup failure: " + e.getMessage());
            return null;
        }
    }

    private void collectMetrics() {
        context.collectAll(new MetricsCollector() {

            @Override
            public void collect(CharSequence key, long value) {
                // just do nothing
            }
        });
    }
}
