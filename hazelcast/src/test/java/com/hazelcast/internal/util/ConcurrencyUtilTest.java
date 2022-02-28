/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcurrencyUtilTest extends HazelcastTestSupport {

    private final Object mutex = new Object();
    private final ContextMutexFactory mutexFactory = new ContextMutexFactory();

    private final IntIntConstructorFunction constructorFunction = new IntIntConstructorFunction();

    private ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConcurrencyUtil.class);
    }

    @Test
    public void setMax() {
        setMax(8, 7);
        setMax(9, 9);
        setMax(10, 11);
    }

    private void setMax(long current, long update) {
        LongValue longValue = new LongValue();
        longValue.value = current;

        ConcurrencyUtil.setMax(longValue, LongValue.UPDATER, update);

        long max = Math.max(current, update);
        assertEquals(max, longValue.value);
    }

    @Test
    public void testGetOrPutSynchronized() {
        int result = ConcurrencyUtil.getOrPutSynchronized(map, 5, mutex, constructorFunction);
        assertEquals(1005, result);

        assertEquals(1, constructorFunction.getConstructions());
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void testGetOrPutSynchronized_whenMutexIsNull_thenThrowException() {
        ConcurrencyUtil.getOrPutSynchronized(map, 5, (Object) null, constructorFunction);
    }

    @Test
    public void testGetOrPutSynchronized_withMutexFactory() {
        int result = ConcurrencyUtil.getOrPutSynchronized(map, 5, mutexFactory, constructorFunction);
        assertEquals(1005, result);

        assertEquals(1, constructorFunction.getConstructions());
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void testGetOrPutSynchronized_whenMutexFactoryIsNull_thenThrowException() {
        ContextMutexFactory factory = null;
        ConcurrencyUtil.getOrPutSynchronized(map, 5, factory, constructorFunction);
    }

    @Test
    public void testSetIfEqualOrGreaterThan() {
        assertTrue(ConcurrencyUtil.setIfEqualOrGreaterThan(new AtomicLong(1), 1));
        assertTrue(ConcurrencyUtil.setIfEqualOrGreaterThan(new AtomicLong(1), 2));
        assertFalse(ConcurrencyUtil.setIfEqualOrGreaterThan(new AtomicLong(2), 1));
    }

    @Test
    public void testGetOrPutIfAbsent() {
        int result = ConcurrencyUtil.getOrPutIfAbsent(map, 5, constructorFunction);
        assertEquals(1005, result);

        result = ConcurrencyUtil.getOrPutIfAbsent(map, 5, constructorFunction);
        assertEquals(1005, result);

        assertEquals(1, constructorFunction.getConstructions());
    }

    private static final class LongValue {

        static final AtomicLongFieldUpdater UPDATER = AtomicLongFieldUpdater.newUpdater(LongValue.class, "value");

        volatile long value;
    }

    private static class IntIntConstructorFunction implements ConstructorFunction<Integer, Integer> {

        private AtomicInteger constructions = new AtomicInteger();

        @Override
        public Integer createNew(Integer key) {
            constructions.incrementAndGet();
            return key + 1000;
        }

        int getConstructions() {
            return constructions.get();
        }
    }
}
