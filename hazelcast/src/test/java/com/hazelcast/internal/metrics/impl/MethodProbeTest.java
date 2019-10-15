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
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MethodProbe.LongMethodProbe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.impl.MethodProbe.createMethodProbe;
import static com.hazelcast.internal.util.CollectionUtil.getItemAtPositionOrNull;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MethodProbeTest extends HazelcastTestSupport {

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
        getLong("SemaphoreMethod", 10);

        getLong("nullAtomicLongMethod", 0);
        getLong("nullAtomicIntegerMethod", 0);
        getLong("nullCounterMethod", 0);
        getLong("nullCollectionMethod", 0);
        getLong("nullMapMethod", 0);
        getLong("nullByteMethod", 0);
        getLong("nullShortMethod", 0);
        getLong("nullIntegerMethod", 0);
        getLong("nullLongMethod", 0);
        getLong("nullSemaphoreMethod", 0);
    }

    @Test
    public void testGeneratedMethodProbeName_removeGetPrefix() throws NoSuchMethodException {
        SomeSource source = new SomeSource();
        Method method = source.getClass().getDeclaredMethod("getSomeIntegerMethod");
        Probe probe = method.getAnnotation(Probe.class);
        MethodProbe methodProbe = createMethodProbe(method, probe);

        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(mock(ILogger.class), ProbeLevel.DEBUG);
        methodProbe.register(metricsRegistry, source, "prefix");

        Set<String> names = metricsRegistry.getNames();
        assertEquals(1, names.size());
        String probeName = getItemAtPositionOrNull(names, 0);
        assertEquals("[metric=prefix.someIntegerMethod]", probeName);
    }

    public void getLong(String methodName, int expectedValue) throws Exception {
        SomeSource source = new SomeSource();
        Method method = source.getClass().getDeclaredMethod(methodName);
        Probe probe = method.getAnnotation(Probe.class);
        MethodProbe methodProbe = createMethodProbe(method, probe);

        LongMethodProbe longMethodProbe = assertInstanceOf(LongMethodProbe.class, methodProbe);

        long value = longMethodProbe.get(source);

        assertEquals(expectedValue, value);
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
        Probe probe = method.getAnnotation(Probe.class);
        MethodProbe methodProbe = createMethodProbe(method, probe);

        MethodProbe.DoubleMethodProbe doubleMethodProbe = assertInstanceOf(MethodProbe.DoubleMethodProbe.class, methodProbe);
        double value = doubleMethodProbe.get(source);

        assertEquals(expected, value, 0.1);
    }


    private class SomeSource {
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
        private Collection collectionMethod() {
            return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        }

        @Probe
        private Collection nullCollectionMethod() {
            return null;
        }

        @Probe
        private Map mapMethod() {
            return MetricsUtils.createMap(10);
        }

        @Probe
        private Map nullMapMethod() {
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
            return null;
        }
    }
}
