package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import freemarker.ext.util.IdentityHashMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.counters.SwCounter.newSwCounter;
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
        metricsRegistry.scanAndRegister(object, "foo");
    }

    public class MethodWithArgument {
        @Probe
        private long method(int x) {
            return 10;
        }
    }

    @Test
    public void register_withCustomName() {
        GaugeMethodWithName object = new GaugeMethodWithName();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.mymethod");
        assertEquals(10, gauge.readLong());
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
        metricsRegistry.scanAndRegister(object, "foo");
    }

    public class VoidMethod {
        @Probe
        private void method() {
        }
    }

    @Test
    public void register_primitiveByte() {
        PrimitiveByteMethod object = new PrimitiveByteMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method(), gauge.readLong());
    }

    public class PrimitiveByteMethod {
        @Probe
        private byte method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveShort() {
        PrimitiveShortMethod object = new PrimitiveShortMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method(), gauge.readLong());
    }

    public class PrimitiveShortMethod {
        @Probe
        private short method() {
            return 10;
        }
    }


    @Test
    public void register_primitiveInt() {
        PrimitiveIntMethod object = new PrimitiveIntMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(10, gauge.readLong());
    }

    public class PrimitiveIntMethod {
        @Probe
        private int method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongMethod object = new PrimitiveLongMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(10, gauge.readLong());
    }


    public class PrimitiveLongMethod {
        @Probe
        private long method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveFloat() {
        PrimitiveFloatMethod object = new PrimitiveFloatMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method(), gauge.readDouble(), 0.1);
    }

    public class PrimitiveFloatMethod {
        @Probe
        private float method() {
            return 10f;
        }
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleMethod object = new PrimitiveDoubleMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method(), gauge.readDouble(), 0.1);
    }

    public class PrimitiveDoubleMethod {
        @Probe
        private double method() {
            return 10d;
        }
    }

    @Test
    public void register_atomicLong() {
        AtomicLongMethod object = new AtomicLongMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().get(), gauge.readLong());
    }

    public class AtomicLongMethod {
        @Probe
        private AtomicLong method() {
            return new AtomicLong(10);
        }
    }

    @Test
    public void register_atomicInteger() {
        AtomicIntegerMethod object = new AtomicIntegerMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().get(), gauge.readLong());
    }

    public class AtomicIntegerMethod {
        @Probe
        private AtomicInteger method() {
            return new AtomicInteger(10);
        }
    }

    @Test
    public void register_counter() {
        CounterMethod object = new CounterMethod();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().get(), gauge.readLong());
    }

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
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().size(), gauge.readLong());
    }

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
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().size(), gauge.readLong());
    }

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
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method().size(), gauge.readLong());
    }

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
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(StaticMethod.method(), gauge.readLong());
    }

    public static class StaticMethod {
        @Probe
        private static long method() {
            return 10;
        }
    }

    @Test
    public void register_interfaceWithGauges() {
        SomeInterfaceImplementation object = new SomeInterfaceImplementation();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.method");
        assertEquals(10, gauge.readLong());
    }

    public  interface SomeInterface{
        @Probe
        int method();
    }

    public static class SomeInterfaceImplementation implements SomeInterface{
        @Override
        public int method() {
            return 10;
        }
    }

    @Test
    public void register_superclassWithGaugeMethods() {
        SubclassWithGauges object = new SubclassWithGauges();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge methodGauge = metricsRegistry.getGauge("foo.method");
        assertEquals(object.method(), methodGauge.readLong());

        Gauge fieldGauge = metricsRegistry.getGauge("foo.field");
        assertEquals(object.field, fieldGauge.readLong());
    }

    public static abstract class ClassWithGauges {
        @Probe
        int method(){
            return 10;
        }

        @Probe
        int field=10;
    }

    public static class SubclassWithGauges extends ClassWithGauges {

    }
}
