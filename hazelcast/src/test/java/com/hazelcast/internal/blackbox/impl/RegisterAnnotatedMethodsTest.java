package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.util.counters.SwCounter;
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

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodWithArguments() {
        MethodWithArgument object = new MethodWithArgument();
        blackbox.scanAndRegister(object, "foo");
    }

    public class MethodWithArgument {
        @SensorInput
        private long method(int x) {
            return 10;
        }
    }

    @Test
    public void register_withCustomName() {
        SensorMethodWithName object = new SensorMethodWithName();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.mymethod");
        assertEquals(10, sensor.readLong());
    }

    public class SensorMethodWithName {
        @SensorInput(name = "mymethod")
        private long method() {
            return 10;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_methodReturnsVoid() {
        VoidMethod object = new VoidMethod();
        blackbox.scanAndRegister(object, "foo");
    }

    public class VoidMethod {
        @SensorInput
        private void method() {
        }
    }

    @Test
    public void register_primitiveByte() {
        PrimitiveByteMethod object = new PrimitiveByteMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method(), sensor.readLong());
    }

    public class PrimitiveByteMethod {
        @SensorInput
        private byte method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveShort() {
        PrimitiveShortMethod object = new PrimitiveShortMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method(), sensor.readLong());
    }

    public class PrimitiveShortMethod {
        @SensorInput
        private short method() {
            return 10;
        }
    }


    @Test
    public void register_primitiveInt() {
        PrimitiveIntMethod object = new PrimitiveIntMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(10, sensor.readLong());
    }

    public class PrimitiveIntMethod {
        @SensorInput
        private int method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveLong() {
        PrimitiveLongMethod object = new PrimitiveLongMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(10, sensor.readLong());
    }


    public class PrimitiveLongMethod {
        @SensorInput
        private long method() {
            return 10;
        }
    }

    @Test
    public void register_primitiveFloat() {
        PrimitiveFloatMethod object = new PrimitiveFloatMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method(), sensor.readDouble(), 0.1);
    }

    public class PrimitiveFloatMethod {
        @SensorInput
        private float method() {
            return 10f;
        }
    }

    @Test
    public void register_primitiveDouble() {
        PrimitiveDoubleMethod object = new PrimitiveDoubleMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method(), sensor.readDouble(), 0.1);
    }

    public class PrimitiveDoubleMethod {
        @SensorInput
        private double method() {
            return 10d;
        }
    }

    @Test
    public void register_atomicLong() {
        AtomicLongMethod object = new AtomicLongMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().get(), sensor.readLong());
    }

    public class AtomicLongMethod {
        @SensorInput
        private AtomicLong method() {
            return new AtomicLong(10);
        }
    }

    @Test
    public void register_atomicInteger() {
        AtomicIntegerMethod object = new AtomicIntegerMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().get(), sensor.readLong());
    }

    public class AtomicIntegerMethod {
        @SensorInput
        private AtomicInteger method() {
            return new AtomicInteger(10);
        }
    }

    @Test
    public void register_counter() {
        CounterMethod object = new CounterMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().get(), sensor.readLong());
    }

    public class CounterMethod {
        @SensorInput
        private Counter method() {
            Counter counter = newSwCounter();
            counter.inc(10);
            return counter;
        }
    }

    @Test
    public void register_collection() {
        CollectionMethod object = new CollectionMethod();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().size(), sensor.readLong());
    }

    public class CollectionMethod {
        @SensorInput
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
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().size(), sensor.readLong());
    }

    public class MapMethod {
        @SensorInput
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
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(object.method().size(), sensor.readLong());
    }

    public class SubclassMethod {
        @SensorInput
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
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(StaticMethod.method(), sensor.readLong());
    }

    public static class StaticMethod {
        @SensorInput
        private static long method() {
            return 10;
        }
    }

    @Test
    public void register_interfaceWithSensors() {
        SomeInterfaceImplementation object = new SomeInterfaceImplementation();
        blackbox.scanAndRegister(object, "foo");

        Sensor sensor = blackbox.getSensor("foo.method");
        assertEquals(10, sensor.readLong());
    }

    public  interface SomeInterface{
        @SensorInput
        int method();
    }

    public static class SomeInterfaceImplementation implements SomeInterface{
        @Override
        public int method() {
            return 10;
        }
    }

    @Test
    public void register_superclassWithSensorMethods() {
        SubclassWithSensor object = new SubclassWithSensor();
        blackbox.scanAndRegister(object, "foo");

        Sensor methodSensor = blackbox.getSensor("foo.method");
        assertEquals(object.method(), methodSensor.readLong());

        Sensor fieldSensor = blackbox.getSensor("foo.field");
        assertEquals(object.field, fieldSensor.readLong());
    }

    public static abstract class ClassWithSensor{
        @SensorInput
        int method(){
            return 10;
        }

        @SensorInput
        int field=10;
    }

    public static class SubclassWithSensor extends ClassWithSensor{

    }
}
