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

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.Namespace;
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
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodWithArguments() {
        MethodWithArgument object = new MethodWithArgument();
        metricsRegistry.register(object);
    }

    @Namespace(name = "foo")
    public class MethodWithArgument {
        @Probe
        private long method(int x) {
            return 10;
        }
    }

    @Test
    public void register_withCustomName() {
        GaugeMethodWithName object = new GaugeMethodWithName();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.mymethod");
        assertEquals(10, gauge.read());
    }

    @Namespace(name = "foo")
    public class GaugeMethodWithName {
        @Probe(name = "mymethod")
        private long method() {
            return 10;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodReturnsVoid() {
        VoidMethod object = new VoidMethod();
        metricsRegistry.register(object);
    }

    @Namespace(name = "foo")
    public class VoidMethod {
        @Probe
        private void method() {
        }
    }

    @Test
    public void register_primitiveByte() {
        PrimitiveByteMethod object = new PrimitiveByteMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), gauge.read());
    }

    @Namespace(name = "foo")
    public class PrimitiveByteMethod {
        @Probe
        private byte method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveShort() {
        PrimitiveShortMethod object = new PrimitiveShortMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), gauge.read());
    }

    @Namespace(name = "foo")
    public class PrimitiveShortMethod {
        @Probe
        private short method() {
            return 10;
        }
    }


    @Test
    public void register_primitiveInt() {
        PrimitiveIntMethod object = new PrimitiveIntMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }

    @Namespace(name = "foo")
    public class PrimitiveIntMethod {
        @Probe
        private int method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongMethod object = new PrimitiveLongMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }

    @Namespace(name = "foo")
    public class PrimitiveLongMethod {
        @Probe
        private long method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveFloat() {
        PrimitiveFloatMethod object = new PrimitiveFloatMethod();
        metricsRegistry.register(object);

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.method");
        assertEquals(object.method(), gauge.read(), 0.1);
    }

    @Namespace(name = "foo")
    public class PrimitiveFloatMethod {
        @Probe
        private float method() {
            return 10f;
        }
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleMethod object = new PrimitiveDoubleMethod();
        metricsRegistry.register(object);

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.method");
        assertEquals(object.method(), gauge.read(), 0.1);
    }

    @Namespace(name = "foo")
    public class PrimitiveDoubleMethod {
        @Probe
        private double method() {
            return 10d;
        }
    }

    @Test
    public void register_atomicLong() {
        AtomicLongMethod object = new AtomicLongMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    @Namespace(name = "foo")
    public class AtomicLongMethod {
        @Probe
        private AtomicLong method() {
            return new AtomicLong(10);
        }
    }

    @Test
    public void register_atomicInteger() {
        AtomicIntegerMethod object = new AtomicIntegerMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    @Namespace(name = "foo")
    public class AtomicIntegerMethod {
        @Probe
        private AtomicInteger method() {
            return new AtomicInteger(10);
        }
    }

    @Test
    public void register_counter() {
        CounterMethod object = new CounterMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().get(), gauge.read());
    }

    @Namespace(name = "foo")
    public class CounterMethod {
        @Probe
        private Counter method() {
            Counter counter = newSwCounter();
            counter.inc(10);
            return counter;
        }
    }

    @Test
    public void register_collection() {
        CollectionMethod object = new CollectionMethod();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    @Namespace(name = "foo")
    public class CollectionMethod {
        @Probe
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
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    @Namespace(name = "foo")
    public class MapMethod {
        @Probe
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
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method().size(), gauge.read());
    }

    @Namespace(name = "foo")
    public class SubclassMethod {
        @Probe
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
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(StaticMethod.method(), gauge.read());
    }

    @Namespace(name = "foo")
    public static class StaticMethod {
        @Probe
        private static long method() {
            return 10;
        }
    }

    @Test
    public void register_interfaceWithGauges() {
        SomeInterfaceImplementation object = new SomeInterfaceImplementation();
        metricsRegistry.register(object);

        LongGauge gauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(10, gauge.read());
    }

    @Namespace(name = "foo")
    public interface SomeInterface {
        @Probe
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
        metricsRegistry.register(object);

        LongGauge methodGauge = metricsRegistry.newLongGauge("foo.method");
        assertEquals(object.method(), methodGauge.read());

        LongGauge fieldGauge = metricsRegistry.newLongGauge("foo.field");
        assertEquals(object.field, fieldGauge.read());
    }

    abstract static class ClassWithGauges {
        @Probe
        int method() {
            return 10;
        }

        @Probe
        int field = 10;
    }

    @Namespace(name = "foo")
    private static class SubclassWithGauges extends ClassWithGauges {
    }
}
