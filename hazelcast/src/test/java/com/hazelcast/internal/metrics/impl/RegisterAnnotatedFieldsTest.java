package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.Metric;
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

import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RegisterAnnotatedFieldsTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void register_customName() {
        ObjectLongGaugeFieldWithName object = new ObjectLongGaugeFieldWithName();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.myfield");
        object.field = 10;
        assertEquals(object.field, gauge.readLong());
    }

    public class ObjectLongGaugeFieldWithName {
        @Probe(name = "myfield")
        private long field;
    }

    @Test
    public void register_primitiveInteger() {
        PrimitiveIntegerField object = new PrimitiveIntegerField();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.readLong());
    }

    public class PrimitiveIntegerField {
        @Probe
        private int field;
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongField object = new PrimitiveLongField();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.readLong());
    }

    public class PrimitiveLongField {
        @Probe
        private long field;
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleField object = new PrimitiveDoubleField();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        object.field = 10;
        assertEquals(object.field, gauge.readDouble(),0.1);
    }

    public class PrimitiveDoubleField {
        @Probe
        private double field;
    }

    @Test
    public void register_concurrentHashMap() {
        ConcurrentMapField object = new ConcurrentMapField();
        object.field.put("foo", "foo");
        object.field.put("bar", "bar");
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        assertEquals(object.field.size(), gauge.readLong());

        object.field = null;
        assertEquals(0, gauge.readLong());
    }

    public class ConcurrentMapField {
        @Probe
        private ConcurrentHashMap field = new ConcurrentHashMap();
    }

    @Test
    public void register_counterFields() {
        CounterField object = new CounterField();
        object.field.inc(10);
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        assertEquals(10, gauge.readLong());

        object.field = null;
        assertEquals(0, gauge.readLong());
    }

    public class CounterField {
        @Probe
        private Counter field = newSwCounter();
    }

    @Test
    public void register_staticField() {
        StaticField object = new StaticField();
        StaticField.field.set(10);
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        assertEquals(10, gauge.readLong());

        StaticField.field = null;
        assertEquals(0, gauge.readLong());
    }

    public static class StaticField {
        @Probe
        static AtomicInteger field = new AtomicInteger();
    }

    @Test
    public void register_superclassRegistration() {
        Subclass object = new Subclass();
        metricsRegistry.scanAndRegister(object, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.field");
        assertEquals(0, gauge.readLong());

        object.field = 10;
        assertEquals(10, gauge.readLong());
    }

    public static class SuperClass{
        @Probe
        int field;
    }

    public static class Subclass extends SuperClass{

    }
}
