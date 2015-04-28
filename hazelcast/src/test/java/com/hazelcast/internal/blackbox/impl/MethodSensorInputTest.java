package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.util.counters.SwCounter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MethodSensorInputTest {

    @Test
    public void getLong() throws Exception {
        getLong("byteMethod", 10);
        getLong("shortMethod", 10);
        getLong("intMethod", 10);
        getLong("longMethod", 10);
        getLong("atomicLongMethod", 10);
        getLong("atomicIntegerMethod", 10);
        getLong("counterMethod", 10);
        getLong("collectionMethod", 10);
        getLong("mapMethod", 10);

        getLong("ByteMethod", 10);
        getLong("ShortMethod", 10);
        getLong("IntegerMethod", 10);
        getLong("LongMethod", 10);

        getLong("nullAtomicLongMethod", 0);
        getLong("nullAtomicIntegerMethod", 0);
        getLong("nullCounterMethod", 0);
        getLong("nullCollectionMethod", 0);
        getLong("nullMapMethod", 0);
        getLong("nullByteMethod", 0);
        getLong("nullShortMethod", 0);
        getLong("nullIntegerMethod", 0);
        getLong("nullLongMethod", 0);
    }

    public void getLong(String fieldName, int expectedValue) throws Exception {
        SomeSource source = new SomeSource();
        Method method = source.getClass().getDeclaredMethod(fieldName);
        SensorInput sensorInput = method.getAnnotation(SensorInput.class);
        MethodSensorInput input = new MethodSensorInput(method, sensorInput);
        assertFalse(input.isDouble());

        long value = input.getLong(source);

        assertEquals(expectedValue, value);
    }

    @Test
    public void getLong_whenDouble() throws Exception {
        getLong_whenDouble("floatMethod");
        getLong_whenDouble("doubleMethod");
    }

    public void getLong_whenDouble(String fieldName) throws Exception {
        SomeSource source = new SomeSource();
        Method method = source.getClass().getDeclaredMethod(fieldName);
        SensorInput sensorInput = method.getAnnotation(SensorInput.class);
        MethodSensorInput fieldSensorInput = new MethodSensorInput(method, sensorInput);

        try {
            fieldSensorInput.getLong(source);
            fail();
        } catch (IllegalStateException expected) {

        }
    }

    @Test
    public void getDouble() throws Exception {
        getDouble("floatMethod", 10);
        getDouble("doubleMethod", 10);
        getDouble("DoubleMethod", 10);
        getDouble("FloatMethod", 10);
        getDouble("nullDoubleMethod", 0);
        getDouble("nullFloatMethod", 0);
    }

    public void getDouble(String fieldName, double expected) throws Exception {
        SomeSource source = new SomeSource();
        Method method = source.getClass().getDeclaredMethod(fieldName);
        SensorInput sensorInput = method.getAnnotation(SensorInput.class);

        MethodSensorInput fieldSensorInput = new MethodSensorInput(method, sensorInput);
        assertTrue(fieldSensorInput.isDouble());

        double value = fieldSensorInput.getDouble(source);

        assertEquals(expected, value, 0.1);
    }

    @Test
    public void getDouble_whenLong() throws Exception {
        getDouble_whenLong("byteMethod");
        getDouble_whenLong("shortMethod");
        getDouble_whenLong("intMethod");
        getDouble_whenLong("longMethod");
        getDouble_whenLong("atomicLongMethod");
        getDouble_whenLong("atomicIntegerMethod");
        getDouble_whenLong("counterMethod");
        getDouble_whenLong("collectionMethod");
    }

    public void getDouble_whenLong(String fieldName) throws Exception {
        SomeSource source = new SomeSource();
        Method field = source.getClass().getDeclaredMethod(fieldName);
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);
        MethodSensorInput fieldSensorInput = new MethodSensorInput(field, sensorInput);

        try {
            fieldSensorInput.getDouble(source);
            fail();
        } catch (IllegalStateException expected) {

        }
    }

    private class SomeSource {
        @SensorInput
        private byte byteMethod() {
            return 10;
        }

        @SensorInput
        private short shortMethod() {
            return 10;
        }

        @SensorInput
        private int intMethod() {
            return 10;
        }

        @SensorInput
        private long longMethod() {
            return 10;
        }

        @SensorInput
        private float floatMethod() {
            return 10;
        }

        @SensorInput
        private double doubleMethod() {
            return 10;
        }

        @SensorInput
        private AtomicLong atomicLongMethod() {
            return new AtomicLong(10);

        }

        @SensorInput
        private AtomicLong nullAtomicLongMethod() {
            return null;
        }

        @SensorInput
        private AtomicInteger atomicIntegerMethod() {
            return new AtomicInteger(10);
        }

        @SensorInput
        private AtomicInteger nullAtomicIntegerMethod() {
            return null;
        }

        @SensorInput
        private Counter counterMethod() {
            return newSwCounter(10);
        }

        @SensorInput
        private Counter nullCounterMethod() {
            return null;
        }

        @SensorInput
        private Collection collectionMethod() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        }

        @SensorInput
        private Collection nullCollectionMethod() {
            return null;
        }

        @SensorInput
        private Map mapMethod() {
            return BlackboxUtils.createMap(10);
        }

        @SensorInput
        private Map nullMapMethod() {
            return null;
        }

        @SensorInput
        private Byte ByteMethod() {
            return (byte) 10;
        }

        @SensorInput
        private Short ShortMethod() {
            return (short) 10;
        }

        @SensorInput
        private Integer IntegerMethod() {
            return 10;
        }

        @SensorInput
        private Long LongMethod() {
            return (long) 10;
        }

        @SensorInput
        private Float FloatMethod() {
            return (float) 10;
        }

        @SensorInput
        private Double DoubleMethod() {
            return (double) 10;
        }

        @SensorInput
        private Byte nullByteMethod() {
            return null;
        }

        @SensorInput
        private Short nullShortMethod() {
            return null;
        }

        @SensorInput
        private Integer nullIntegerMethod() {
            return null;
        }

        @SensorInput
        private Long nullLongMethod() {
            return null;
        }

        @SensorInput
        private Float nullFloatMethod() {
            return null;
        }

        @SensorInput
        private Double nullDoubleMethod() {
            return null;
        }
    }
}
