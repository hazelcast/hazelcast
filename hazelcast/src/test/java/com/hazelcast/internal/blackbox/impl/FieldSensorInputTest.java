package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.util.counters.Counter;
import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.util.counters.SwCounter;
import org.junit.Test;

import java.lang.reflect.Field;
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


public class FieldSensorInputTest {

    @Test
    public void whenUnknownType() throws NoSuchFieldException {
        UnknownFieldType unknownFieldType= new UnknownFieldType();
        Field field = unknownFieldType.getClass().getDeclaredField("field");
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);

        try {
            new FieldSensorInput(field, sensorInput);
            fail();
        }catch (IllegalArgumentException e){
        }
    }

    private class UnknownFieldType{
        @SensorInput
        private String field;
    }

    @Test
    public void getLong() throws Exception {
        getLong("byteField", 10);
        getLong("shortField", 10);
        getLong("intField", 10);
        getLong("longField", 10);
        getLong("atomicLongField", 10);
        getLong("atomicIntegerField", 10);
        getLong("counterField", 10);
        getLong("collectionField", 10);
        getLong("mapField", 10);

        getLong("ByteField", 10);
        getLong("ShortField", 10);
        getLong("IntegerField", 10);
        getLong("LongField", 10);

        getLong("nullAtomicLongField", 0);
        getLong("nullAtomicIntegerField", 0);
        getLong("nullCounterField", 0);
        getLong("nullCollectionField", 0);
        getLong("nullMapField", 0);
        getLong("nullByteField", 0);
        getLong("nullShortField", 0);
        getLong("nullIntegerField", 0);
        getLong("nullLongField", 0);
    }

    public void getLong(String fieldName, int expectedValue) throws Exception {
        SomeSource source = new SomeSource();
        Field field = source.getClass().getDeclaredField(fieldName);
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);
        FieldSensorInput fieldSensorInput = new FieldSensorInput(field, sensorInput);
        assertFalse(fieldSensorInput.isDouble());

        long value = fieldSensorInput.getLong(source);

        assertEquals(expectedValue, value);
    }

    @Test
    public void getLong_whenDouble() throws Exception {
        getLong_whenDouble("floatField");
        getLong_whenDouble("doubleField");
    }

    public void getLong_whenDouble(String fieldName) throws Exception {
        SomeSource source = new SomeSource();
        Field field = source.getClass().getDeclaredField(fieldName);
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);
        FieldSensorInput fieldSensorInput = new FieldSensorInput(field, sensorInput);

        try {
            fieldSensorInput.getLong(source);
            fail();
        } catch (IllegalStateException expected) {

        }
    }

    @Test
    public void getDouble() throws Exception {
        getDouble("floatField", 10);
        getDouble("doubleField", 10);
        getDouble("DoubleField", 10);
        getDouble("FloatField", 10);
        getDouble("nullDoubleField", 0);
        getDouble("nullFloatField", 0);
    }

    public void getDouble(String fieldName, double expected) throws Exception {
        SomeSource source = new SomeSource();
        Field field = source.getClass().getDeclaredField(fieldName);
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);

        FieldSensorInput fieldSensorInput = new FieldSensorInput(field, sensorInput);
        assertTrue(fieldSensorInput.isDouble());

        double value = fieldSensorInput.getDouble(source);

        assertEquals(expected, value, 0.1);
    }

    @Test
    public void getDouble_whenLong() throws Exception {
        getDouble_whenLong("byteField");
        getDouble_whenLong("shortField");
        getDouble_whenLong("intField");
        getDouble_whenLong("longField");
        getDouble_whenLong("atomicLongField");
        getDouble_whenLong("atomicIntegerField");
        getDouble_whenLong("counterField");
        getDouble_whenLong("collectionField");
    }

    public void getDouble_whenLong(String fieldName) throws Exception {
        SomeSource source = new SomeSource();
        Field field = source.getClass().getDeclaredField(fieldName);
        SensorInput sensorInput = field.getAnnotation(SensorInput.class);
        FieldSensorInput fieldSensorInput = new FieldSensorInput(field, sensorInput);

        try {
            fieldSensorInput.getDouble(source);
            fail();
        } catch (IllegalStateException expected) {

        }
    }

    private class SomeSource {
        @SensorInput
        private byte byteField = 10;
        @SensorInput
        private short shortField = 10;
        @SensorInput
        private int intField = 10;
        @SensorInput
        private long longField = 10;

        @SensorInput
        private float floatField = 10;
        @SensorInput
        private double doubleField = 10;

        @SensorInput
        private AtomicLong atomicLongField = new AtomicLong(10);
        @SensorInput
        private AtomicLong nullAtomicLongField;
        @SensorInput
        private AtomicInteger atomicIntegerField = new AtomicInteger(10);
        @SensorInput
        private AtomicInteger nullAtomicIntegerField;
        @SensorInput
        private Counter counterField = newSwCounter(10);
        @SensorInput
        private Counter nullCounterField;
        @SensorInput
        private Collection collectionField = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        @SensorInput
        private Collection nullCollectionField;
        @SensorInput
        private Map mapField = BlackboxUtils.createMap(10);
        @SensorInput
        private Map nullMapField;


        @SensorInput
        private Byte ByteField = (byte) 10;
        @SensorInput
        private Short ShortField = (short) 10;
        @SensorInput
        private Integer IntegerField = 10;
        @SensorInput
        private Long LongField = (long) 10;
        @SensorInput
        private Float FloatField = (float) 10;
        @SensorInput
        private Double DoubleField = (double) 10;

        @SensorInput
        private Byte nullByteField;
        @SensorInput
        private Short nullShortField;
        @SensorInput
        private Integer nullIntegerField;
        @SensorInput
        private Long nullLongField;
        @SensorInput
        private Float nullFloatField;
        @SensorInput
        private Double nullDoubleField;
    }
}
