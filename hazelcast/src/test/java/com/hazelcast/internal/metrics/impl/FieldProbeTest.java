/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.impl.FieldProbe.DoubleFieldProbe;
import com.hazelcast.internal.metrics.impl.FieldProbe.LongFieldProbe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.impl.FieldProbe.createFieldProbe;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FieldProbeTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void whenUnknownType() throws NoSuchFieldException {
        UnknownFieldType unknownFieldType = new UnknownFieldType();
        Field field = unknownFieldType.getClass().getDeclaredField("field");
        Probe probe = field.getAnnotation(Probe.class);

        createFieldProbe(field, probe);
    }

    private class UnknownFieldType {
        @Probe
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
        getLong("semaphoreField", 10);

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
        getLong("nullSemaphoreField", 0);
    }

    public void getLong(String fieldName, int expectedValue) throws Exception {
        SomeSource source = new SomeSource();
        Field field = source.getClass().getDeclaredField(fieldName);
        Probe probe = field.getAnnotation(Probe.class);
        FieldProbe fieldProbe = createFieldProbe(field, probe);

        LongFieldProbe longFieldProbe = assertInstanceOf(LongFieldProbe.class, fieldProbe);

        long value = longFieldProbe.get(source);

        assertEquals(expectedValue, value);
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
        Probe probe = field.getAnnotation(Probe.class);

        FieldProbe fieldProbe = createFieldProbe(field, probe);
        assertInstanceOf(DoubleFieldProbe.class, fieldProbe);

        DoubleFieldProbe doubleFieldProbe = (DoubleFieldProbe) fieldProbe;

        double value = doubleFieldProbe.get(source);

        assertEquals(expected, value, 0.1);
    }

    private class SomeSource {
        @Probe
        private byte byteField = 10;
        @Probe
        private short shortField = 10;
        @Probe
        private int intField = 10;
        @Probe
        private long longField = 10;

        @Probe
        private float floatField = 10;
        @Probe
        private double doubleField = 10;

        @Probe
        private AtomicLong atomicLongField = new AtomicLong(10);
        @Probe
        private AtomicLong nullAtomicLongField;
        @Probe
        private AtomicInteger atomicIntegerField = new AtomicInteger(10);
        @Probe
        private AtomicInteger nullAtomicIntegerField;
        @Probe
        private Counter counterField = newSwCounter(10);
        @Probe
        private Counter nullCounterField;
        @Probe
        private Collection collectionField = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        @Probe
        private Collection nullCollectionField;
        @Probe
        private Map mapField = MetricsUtils.createMap(10);
        @Probe
        private Map nullMapField;
        @Probe
        private Semaphore semaphoreField = new Semaphore(10);
        @Probe
        private Semaphore nullSemaphoreField;

        @Probe
        private Byte ByteField = (byte) 10;
        @Probe
        private Short ShortField = (short) 10;
        @Probe
        private Integer IntegerField = 10;
        @Probe
        private Long LongField = (long) 10;
        @Probe
        private Float FloatField = (float) 10;
        @Probe
        private Double DoubleField = (double) 10;

        @Probe
        private Byte nullByteField;
        @Probe
        private Short nullShortField;
        @Probe
        private Integer nullIntegerField;
        @Probe
        private Long nullLongField;
        @Probe
        private Float nullFloatField;
        @Probe
        private Double nullDoubleField;

    }
}
