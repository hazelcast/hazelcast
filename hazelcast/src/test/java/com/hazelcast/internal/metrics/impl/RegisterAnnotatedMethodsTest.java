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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;

//todo: testing of null return value
//todo: testing of exception return value
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RegisterAnnotatedMethodsTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodWithArguments() {
        MethodWithArgument object = new MethodWithArgument();
        metricsRegistry.registerStaticMetrics(object, "foo");
    }

    public class MethodWithArgument {
        @Probe(name = "method")
        private long method(int x) {
            return 10;
        }
    }

    @Test
    public void register_withCustomName() {
        GaugeMethodWithName object = new GaugeMethodWithName();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.mymethod");
        assertEquals(10, gauge.read());
    }

    public class GaugeMethodWithName {
        @Probe(name = "mymethod")
        private long method() {
            return 10;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodReturnsVoid() {
        VoidMethod object = new VoidMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");
    }

    public class VoidMethod {
        @Probe(name = "method")
        private void method() {
        }
    }

    @Test
    public void register_primitiveByte() {
        PrimitiveByteMethod object = new PrimitiveByteMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), gauge.read());
    }

    public class PrimitiveByteMethod {
        @Probe(name = "method")
        private byte method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveShort() {
        PrimitiveShortMethod object = new PrimitiveShortMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), gauge.read());
    }

    public class PrimitiveShortMethod {
        @Probe(name = "method")
        private short method() {
            return 10;
        }
    }


    @Test
    public void register_primitiveInt() {
        PrimitiveIntMethod object = new PrimitiveIntMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }

    public class PrimitiveIntMethod {
        @Probe(name = "method")
        private int method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongMethod object = new PrimitiveLongMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }


    public class PrimitiveLongMethod {
        @Probe(name = "method")
        private long method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveFloat() {
        PrimitiveFloatMethod object = new PrimitiveFloatMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.method");
        assertEquals(object.method(), gauge.read(), 0.1);
    }

    public class PrimitiveFloatMethod {
        @Probe(name = "method")
        private float method() {
            return 10f;
        }
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleMethod object = new PrimitiveDoubleMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.method");
        assertEquals(object.method(), gauge.read(), 0.1);
    }

    public class PrimitiveDoubleMethod {
        @Probe(name = "method")
        private double method() {
            return 10d;
        }
    }

    @Test
    public void register_atomicLong() {
        AtomicLongMethod object = new AtomicLongMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    public class AtomicLongMethod {
        @Probe(name = "method")
        private AtomicLong method() {
            return new AtomicLong(10);
        }
    }

    @Test
    public void register_atomicInteger() {
        AtomicIntegerMethod object = new AtomicIntegerMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    public class AtomicIntegerMethod {
        @Probe(name = "method")
        private AtomicInteger method() {
            return new AtomicInteger(10);
        }
    }

    @Test
    public void register_counter() {
        CounterMethod object = new CounterMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    public class CounterMethod {
        @Probe(name = "method")
        private Counter method() {
            Counter counter = newSwCounter();
            counter.inc(10);
            return counter;
        }
    }

    @Test
    public void register_collection() {
        CollectionMethod object = new CollectionMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    public class CollectionMethod {
        @Probe(name = "method")
        private Collection method() {
            ArrayList list = new ArrayList();
            for (int k = 0; k < 10; k++) {
                list.add(k);
            }
            return list;
        }
    }

    @Test
    public void register_map() {
        MapMethod object = new MapMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    public class MapMethod {
        @Probe(name = "method")
        private Map method() {
            HashMap map = new HashMap();
            for (int k = 0; k < 10; k++) {
                map.put(k, k);
            }
            return map;
        }
    }

    @Test
    public void register_subclass() {
        SubclassMethod object = new SubclassMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    public class SubclassMethod {
        @Probe(name = "method")
        private IdentityHashMap method() {
            IdentityHashMap map = new IdentityHashMap();
            for (int k = 0; k < 10; k++) {
                map.put(k, k);
            }
            return map;
        }
    }

    @Test
    public void register_staticMethod() {
        StaticMethod object = new StaticMethod();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(StaticMethod.method(), gauge.read());
    }

    public static class StaticMethod {
        @Probe(name = "method")
        private static long method() {
            return 10;
        }
    }

    @Test
    public void register_interfaceWithGauges() {
        SomeInterfaceImplementation object = new SomeInterfaceImplementation();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }

    public interface SomeInterface {
        @Probe(name = "method")
        int method();
    }

    public static class SomeInterfaceImplementation implements SomeInterface {
        @Override
        public int method() {
            return 10;
        }
    }

    @Test
    public void register_superclassWithGaugeMethods() {
        SubclassWithGauges object = new SubclassWithGauges();
        metricsRegistry.registerStaticMetrics(object, "foo");

        LongGauge methodGauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), methodGauge.read());

        LongGauge fieldGauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(object.field, fieldGauge.read());
    }

    abstract static class ClassWithGauges {
        @Probe(name = "method")
        int method() {
            return 10;
        }

        @Probe(name = "field")
        int field = 10;
    }

    private static class SubclassWithGauges extends ClassWithGauges {
    }
}
