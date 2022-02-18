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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RegisterAnnotatedFieldsTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test
    public void register_customName() {
        ObjectLongGaugeFieldWithName object = new ObjectLongGaugeFieldWithName();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.myfield");
        object.field = 10;
        assertEquals(object.field, gauge.read());
    }

    public class ObjectLongGaugeFieldWithName {
        @Probe(name = "myfield")
        private long field;
    }

    @Test
    public void register_primitiveInteger() {
        PrimitiveIntegerField object = new PrimitiveIntegerField();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.read());
    }

    public class PrimitiveIntegerField {
        @Probe(name = "field")
        private int field;
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongField object = new PrimitiveLongField();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.read());
    }

    public class PrimitiveLongField {
        @Probe(name = "field")
        private long field;
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleField object = new PrimitiveDoubleField();
        metricsRegistry.registerStaticMetrics(object, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.read(), 0.1);
    }

    public class PrimitiveDoubleField {
        @Probe(name = "field")
        private double field;
    }

    @Test
    public void register_concurrentHashMap() {
        ConcurrentMapField object = new ConcurrentMapField();
        object.field.put("foo", "foo");
        object.field.put("bar", "bar");
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(object.field.size(), gauge.read());

        object.field = null;
        assertEquals(0, gauge.read());
    }

    public class ConcurrentMapField {
        @Probe(name = "field")
        private ConcurrentHashMap field = new ConcurrentHashMap();
    }

    @Test
    public void register_counterFields() {
        CounterField object = new CounterField();
        object.field.inc(10);
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(10, gauge.read());

        object.field = null;
        assertEquals(0, gauge.read());
    }

    public class CounterField {
        @Probe(name = "field")
        private Counter field = newSwCounter();
    }

    @Test
    public void register_staticField() {
        StaticField object = new StaticField();
        StaticField.field.set(10);
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(10, gauge.read());

        StaticField.field = null;
        assertEquals(0, gauge.read());
    }

    public static class StaticField {
        @Probe(name = "field")
        static AtomicInteger field = new AtomicInteger();
    }

    @Test
    public void register_superclassRegistration() {
        Subclass object = new Subclass();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(0, gauge.read());

        object.field = 10;
        assertEquals(10, gauge.read());
    }

    public static class SuperClass {
        @Probe(name = "field")
        int field;
    }

    public static class Subclass extends SuperClass {

    }
}
