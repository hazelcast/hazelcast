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

package com.hazelcast.internal.metrics;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ProbeAnnotatedFields2Test extends AbstractMetricsTest {

    @Before
    public void setupRoots() {
        register(new UnknownFieldType());
        register(new SomeSource());
    }

    @Test
    public void whenUnknownType() {
        assertNotCollected("unknownTypeField");
    }

    private static final class UnknownFieldType {
        @Probe
        private String unknownTypeField;
    }

    @Test
    public void longValueFieldProbes() throws Exception {
        assertLong("byteField", 10);
        assertLong("shortField", 10);
        assertLong("intField", 10);
        assertLong("longField", 10);
        assertLong("atomicLongField", 10);
        assertLong("atomicIntegerField", 10);
        assertLong("counterField", 10);
        assertLong("collectionField", 10);
        assertLong("mapField", 10);
        assertLong("semaphoreField", 10);

        assertLong("ByteField", 10);
        assertLong("ShortField", 10);
        assertLong("IntegerField", 10);
        assertLong("LongField", 10);

        assertLong("nullAtomicLongField", -1);
        assertLong("nullAtomicIntegerField", -1);
        assertLong("nullCounterField", -1);
        assertLong("nullCollectionField", -1);
        assertLong("nullMapField", -1);
        assertLong("nullByteField", -1);
        assertLong("nullShortField", -1);
        assertLong("nullIntegerField", -1);
        assertLong("nullLongField", -1);
        assertLong("nullSemaphoreField", -1);
    }

    private void assertLong(String fieldName, long expectedValue) throws Exception {
        assertCollected(fieldName, expectedValue);
    }

    @Test
    public void doubleValueFieldProbes() throws Exception {
        assertDouble("floatField", 10d);
        assertDouble("doubleField", 10d);
        assertDouble("DoubleField", 10d);
        assertDouble("FloatField", 10d);
        assertDouble("nullDoubleField", -1d);
        assertDouble("nullFloatField", -1d);
    }

    private void assertDouble(String fieldName, double expected) throws Exception {
        assertCollected(fieldName, expected == -1d ? -1L : ProbeUtils.toLong(expected));
    }

    private static final class SomeSource {
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
        private Collection<Integer> collectionField = asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        @Probe
        private Collection<String> nullCollectionField;
        @Probe
        private Map<String, Integer> mapField = createMap(10);
        @Probe
        private Map<Integer, Integer> nullMapField;
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
