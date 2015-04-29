package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.util.counters.SwCounter;
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

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test
    public void register_customName() {
        ObjectLongSensorFieldWithName object = new ObjectLongSensorFieldWithName();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.myfield");
        object.field = 10;
        assertEquals(object.field, sensor.readLong());
    }

    public class ObjectLongSensorFieldWithName {
        @SensorInput(name = "myfield")
        private long field;
    }

    @Test
    public void register_primitiveInteger() {
        PrimitiveIntegerField object = new PrimitiveIntegerField();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        object.field = 10;
        assertEquals(object.field, sensor.readLong());
    }

    public class PrimitiveIntegerField {
        @SensorInput
        private int field;
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongField object = new PrimitiveLongField();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        object.field = 10;
        assertEquals(object.field, sensor.readLong());
    }

    public class PrimitiveLongField {
        @SensorInput
        private long field;
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleField object = new PrimitiveDoubleField();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        object.field = 10;
        assertEquals(object.field, sensor.readDouble(),0.1);
    }

    public class PrimitiveDoubleField {
        @SensorInput
        private double field;
    }

    @Test
    public void register_concurrentHashMap() {
        ConcurrentMapField object = new ConcurrentMapField();
        object.field.put("foo", "foo");
        object.field.put("bar", "bar");
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        assertEquals(object.field.size(), sensor.readLong());

        object.field = null;
        assertEquals(0, sensor.readLong());
    }

    public class ConcurrentMapField {
        @SensorInput
        private ConcurrentHashMap field = new ConcurrentHashMap();
    }

    @Test
    public void register_counterSensorField() {
        CounterField object = new CounterField();
        object.field.inc(10);
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        assertEquals(10, sensor.readLong());

        object.field = null;
        assertEquals(0, sensor.readLong());
    }

    public class CounterField {
        @SensorInput
        private Counter field = newSwCounter();
    }

    @Test
    public void register_staticField() {
        StaticField object = new StaticField();
        StaticField.field.set(10);
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        assertEquals(10, sensor.readLong());

        StaticField.field = null;
        assertEquals(0, sensor.readLong());
    }

    public static class StaticField {
        @SensorInput
        static AtomicInteger field = new AtomicInteger();
    }

    @Test
    public void register_superclassRegistration() {
        Subclass object = new Subclass();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.field");
        assertEquals(0, sensor.readLong());

        object.field = 10;
        assertEquals(10, sensor.readLong());
    }

    public static class SuperClass{
        @SensorInput
        int field;
    }

    public static class Subclass extends SuperClass{

    }
}
