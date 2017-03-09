/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static java.lang.Math.round;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongGaugeImplTest {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    class SomeObject {
        @Probe
        long longField = 10;
        @Probe
        double doubleField = 10.8;
    }

    @Test
    public void getName() {
        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        String actual = gauge.getName();

        assertEquals("foo", actual);
    }

    //  ============ getLong ===========================

    @Test
    public void whenNoProbeSet() {
        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(0, actual);
    }

    @Test
    public void whenDoubleProbe() {
        metricsRegistry.register(this, "foo", MANDATORY,
                new DoubleProbeFunction<LongGaugeImplTest>() {
                    @Override
                    public double get(LongGaugeImplTest source) throws Exception {
                        return 10;
                    }
                });

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(10, actual);
    }

    @Test
    public void whenLongProbe() {
        metricsRegistry.register(this, "foo", MANDATORY,
                new LongProbeFunction() {
                    @Override
                    public long get(Object o) throws Exception {
                        return 10;
                    }
                });

        LongGauge gauge = metricsRegistry.newLongGauge("foo");
        assertEquals(10, gauge.read());
    }

    @Test
    public void whenProbeThrowsException() {
        metricsRegistry.register(this, "foo", MANDATORY,
                new LongProbeFunction() {
                    @Override
                    public long get(Object o) {
                        throw new RuntimeException();
                    }
                });

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(0, actual);
    }

    @Test
    public void whenLongProbeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.longField");
        assertEquals(10, gauge.read());
    }

    @Test
    public void whenDoubleProbeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.doubleField");
        assertEquals(round(someObject.doubleField), gauge.read());
    }

    @Test
    public void whenReregister() {
        metricsRegistry.register(this, "foo", MANDATORY,
                new LongProbeFunction() {
                    @Override
                    public long get(Object o) throws Exception {
                        return 10;
                    }
                });

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        gauge.read();

        metricsRegistry.register(this, "foo", MANDATORY,
                new LongProbeFunction() {
                    @Override
                    public long get(Object o) throws Exception {
                        return 11;
                    }
                });

        assertEquals(11, gauge.read());
    }
}
