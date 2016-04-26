/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ItemCounterTest extends HazelcastTestSupport {

    private ItemCounter<Object> counter;

    @Before
    public void setUp() {
        counter = new ItemCounter<Object>();
    }

    @Test
    public void testKeySet() {
        counter.add("key1", 1);
        counter.add("key2", 1);

        assertEquals(new HashSet(asList("key1", "key2")), counter.keySet());
    }

    @Test
    public void testDescendingKeys() {
        counter.add("key1", 2);
        counter.add("key2", 3);
        counter.add("key3", 1);

        assertEquals(asList("key2", "key1", "key3"), counter.descendingKeys());
    }

    @Test
    public void testGet_returnsZeroWhenEmpty() throws Exception {
        long count = counter.get(new Object());
        assertEquals(0, count);
    }

    @Test
    public void testGet_returnsPreviouslySetValue() throws Exception {
        long value = Long.MAX_VALUE;
        Object object = new Object();

        counter.set(object, value);
        long count = counter.get(object);

        assertEquals(value, count);
    }

    @Test
    public void testSet_overridePreviousValue() throws Exception {
        long value = Long.MAX_VALUE;
        Object object = new Object();

        counter.set(object, Long.MIN_VALUE);
        counter.set(object, value);
        long count = counter.get(object);

        assertEquals(value, count);
    }

    @Test
    public void testAdd_whenNoPreviousValueExist() throws Exception {
        Object object = new Object();
        long delta = 1;

        counter.add(object, delta);
        long count = counter.get(object);

        assertEquals(delta, count);
    }

    @Test
    public void testAdd_increaseWhenPreviousValueDoesExist() throws Exception {
        Object object = new Object();
        long initialValue = 1;
        long delta = 1;

        counter.set(object, initialValue);
        counter.add(object, delta);
        long count = counter.get(object);

        assertEquals(delta + initialValue, count);
    }

    @Test
    public void testReset_allValuesAreSetToZeroOnReset() throws Exception {
        Object object1 = new Object();
        Object object2 = new Object();
        long initialValue1 = Long.MAX_VALUE;
        long initialValue2 = Long.MIN_VALUE;

        counter.set(object1, initialValue1);
        counter.set(object2, initialValue2);
        counter.reset();

        long count1 = counter.get(object1);
        long count2 = counter.get(object1);

        assertEquals(0, count1);
        assertEquals(0, count2);
    }

    @Test
    public void testGetAndSet_asSetWhenNoPreviousValueExist() throws Exception {
        Object object = new Object();
        long newValue = Long.MAX_VALUE;

        long count = counter.getAndSet(object, newValue);
        assertEquals(count, 0);

        count = counter.get(object);
        assertEquals(newValue, count);
    }

    @Test
    public void testGetAndSet_overridePreviousValue() throws Exception {
        Object object = new Object();
        long initialValue = Long.MIN_VALUE;
        long newValue = Long.MAX_VALUE;

        counter.set(object, initialValue);

        long count = counter.getAndSet(object, newValue);
        assertEquals(count, initialValue);

        count = counter.get(object);
        assertEquals(newValue, count);
    }

    @Test
    public void testEquals_returnsTrueOnSameInstance() throws Exception {
        assertTrue(counter.equals(counter));
    }

    @Test
    public void testEquals_returnsFalseOnNull() throws Exception {
        assertFalse(counter.equals(null));
    }

    @Test
    public void testEquals_returnsFalseDifferentClass() throws Exception {
        assertFalse(counter.equals(new Object()));
    }

    @Test
    public void testEquals_returnsTrueOnTheSameData() throws Exception {
        Object object1 = new Object();
        ItemCounter<Object> otherCounter = new ItemCounter<Object>();

        counter.set(object1, Long.MAX_VALUE);
        otherCounter.set(object1, Long.MAX_VALUE);

        assertTrue(counter.equals(otherCounter));
    }

    @Test
    public void testEquals_returnsFalseOnTheDifferentData() throws Exception {
        Object object1 = new Object();
        ItemCounter<Object> otherCounter = new ItemCounter<Object>();

        counter.set(object1, Long.MAX_VALUE);
        otherCounter.set(object1, Long.MIN_VALUE);

        assertFalse(counter.equals(otherCounter));
    }

    @Test
    public void testHashCode_doesNotThrowExceptionWhenEmpty() throws Exception {
        counter.hashCode();
    }

    @Test
    public void testHashCode_sameHashCodeOnTheSameData() throws Exception {
        ItemCounter<Object> otherCounter = new ItemCounter<Object>();

        int hashCode = counter.hashCode();
        int otherHashCode = otherCounter.hashCode();

        assertEquals(hashCode, otherHashCode);
    }
}