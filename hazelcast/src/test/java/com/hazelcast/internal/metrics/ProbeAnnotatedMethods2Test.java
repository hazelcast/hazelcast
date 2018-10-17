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

import java.util.Arrays;
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
public class ProbeAnnotatedMethods2Test extends AbstractMetricsTest {

    @Before
    public void setupRoots() {
        register(new SomeSource());
    }

    @Test
    public void longValueMethodProbes() {
        assertLong("byteMethod", 10);
        assertLong("shortMethod", 10);
        assertLong("intMethod", 10);
        assertLong("longMethod", 10);
        assertLong("atomicLongMethod", 10);
        assertLong("atomicIntegerMethod", 10);
        assertLong("counterMethod", 10);
        assertLong("collectionMethod", 10);
        assertLong("mapMethod", 10);

        assertLong("ByteMethod", 10);
        assertLong("ShortMethod", 10);
        assertLong("IntegerMethod", 10);
        assertLong("LongMethod", 10);
        assertLong("SemaphoreMethod", 10);
    }

    @Test
    public void nullLongValueMethodProbes() {
        assertLong("nullAtomicLongMethod", -1);
        assertLong("nullAtomicIntegerMethod", -1);
        assertLong("nullCounterMethod", -1);
        assertLong("nullCollectionMethod", -1);
        assertLong("nullMapMethod", -1);
        assertLong("nullByteMethod", -1);
        assertLong("nullShortMethod", -1);
        assertLong("nullIntegerMethod", -1);
        assertLong("nullLongMethod", -1);
        assertLong("nullSemaphoreMethod", -1);
    }

    @Test
    public void probeNameRemovesGetPrefix() {
        assertLong("someIntegerMethod", 10);
    }

    private void assertLong(String methodName, int expectedValue) {
        assertCollected(methodName, expectedValue);
    }

    @Test
    public void doubleValueMethodProbes() {
        assertDouble("floatMethod", 10);
        assertDouble("doubleMethod", 10);
        assertDouble("DoubleMethod", 10);
        assertDouble("FloatMethod", 10);
    }

    @Test
    public void nullDoubleValueMethodProbes() {
        assertDouble("nullDoubleMethod", -1d);
        assertDouble("nullFloatMethod", -1d);
    }

    private void assertDouble(String fieldName, double expected) {
        assertCollected(fieldName, expected == -1d ? -1L : ProbeUtils.toLong(expected));
    }

    private static final class SomeSource {
        @Probe
        private byte byteMethod() {
            return 10;
        }

        @Probe
        private short shortMethod() {
            return 10;
        }

        @Probe
        private int intMethod() {
            return 10;
        }

        @Probe
        private long longMethod() {
            return 10;
        }

        @Probe
        private float floatMethod() {
            return 10;
        }

        @Probe
        private double doubleMethod() {
            return 10;
        }

        @Probe
        private AtomicLong atomicLongMethod() {
            return new AtomicLong(10);

        }

        @Probe
        private AtomicLong nullAtomicLongMethod() {
            return null;
        }

        @Probe
        private AtomicInteger atomicIntegerMethod() {
            return new AtomicInteger(10);
        }

        @Probe
        private AtomicInteger nullAtomicIntegerMethod() {
            return null;
        }

        @Probe
        private Counter counterMethod() {
            return newSwCounter(10);
        }

        @Probe
        private Counter nullCounterMethod() {
            return null;
        }

        @Probe
        private Collection<Integer> collectionMethod() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        }

        @Probe
        private Collection<Integer> nullCollectionMethod() {
            return null;
        }

        @Probe
        private Map<String, Integer> mapMethod() {
            return createMap(10);
        }

        @Probe
        private Map<Integer, Integer> nullMapMethod() {
            return null;
        }

        @Probe
        private Byte ByteMethod() {
            return (byte) 10;
        }

        @Probe
        private Short ShortMethod() {
            return (short) 10;
        }

        @Probe
        private Integer IntegerMethod() {
            return 10;
        }

        @Probe
        private Long LongMethod() {
            return (long) 10;
        }

        @Probe
        private Float FloatMethod() {
            return (float) 10;
        }

        @Probe
        private Double DoubleMethod() {
            return (double) 10;
        }

        @Probe
        private Semaphore SemaphoreMethod() {
            return new Semaphore(10);
        }

        @Probe
        private Byte nullByteMethod() {
            return null;
        }

        @Probe
        private Short nullShortMethod() {
            return null;
        }

        @Probe
        private Integer nullIntegerMethod() {
            return null;
        }

        @Probe
        private Long nullLongMethod() {
            return null;
        }

        @Probe
        private Float nullFloatMethod() {
            return null;
        }

        @Probe
        private Double nullDoubleMethod() {
            return null;
        }

        @Probe
        private Semaphore nullSemaphoreMethod() {
            return null;
        }

        @Probe
        private Integer getSomeIntegerMethod() {
            return 10;
        }
    }
}
