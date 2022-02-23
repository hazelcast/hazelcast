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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.cache.impl.DeferredValue.asPassThroughSet;
import static com.hazelcast.cache.impl.DeferredValue.concurrentSetOfValues;
import static com.hazelcast.cache.impl.DeferredValue.withSerializedValue;
import static com.hazelcast.cache.impl.DeferredValue.withValue;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeferredValueTest {

    private SerializationService serializationService;
    private String expected;
    private Data serializedValue;

    // for deferred value sets tests
    private Set<String> valueSet;
    private Set<DeferredValue<String>> deferredSet;
    // adaptedSet is backed by deferredSet
    private Set<String> adaptedSet;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        expected = randomString();
        serializedValue = serializationService.toData(expected);

        valueSet = new HashSet<String>();
        valueSet.add("1");
        valueSet.add("2");
        valueSet.add("3");

        deferredSet = concurrentSetOfValues(valueSet);
        adaptedSet = asPassThroughSet(deferredSet, serializationService);
    }

    @Test
    public void testValue_isSame_whenConstructedWithValue() {
        DeferredValue<String> deferredValue = withValue(expected);
        assertSame(expected, deferredValue.get(serializationService));
    }

    @Test
    public void testValue_whenConstructedWithSerializedValue() {
        DeferredValue<String> deferredValue = DeferredValue.withSerializedValue(serializedValue);
        assertEquals(expected, deferredValue.get(serializationService));
    }

    @Test
    public void testSerializedValue_isSame_whenConstructedWithSerializedValue() {
        DeferredValue<String> deferredValue = DeferredValue.withSerializedValue(serializedValue);
        assertSame(serializedValue, deferredValue.getSerializedValue(serializationService));
    }

    @Test
    public void testSerializedValue_whenConstructedWithValue() {
        DeferredValue<String> deferredValue = withValue(expected);
        assertEquals(serializedValue, deferredValue.getSerializedValue(serializationService));
    }

    @Test
    public void testEquals_WithValue() {
        DeferredValue<String> v1 = withValue(expected);
        DeferredValue<String> v2 = withValue(expected);
        assertEquals(v1, v2);
    }

    @Test
    public void testEquals_WithSerializedValue() {
        DeferredValue<String> v1 = DeferredValue.withSerializedValue(serializedValue);
        DeferredValue<String> v2 = DeferredValue.withSerializedValue(serializedValue);
        assertEquals(v1, v2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEquals_WithValueAndSerializedValue() {
        DeferredValue<String> v1 = withValue(expected);
        DeferredValue<String> v2 = DeferredValue.withSerializedValue(serializedValue);
        assertEquals(v1, v2);
    }

    @Test
    public void testNullValue_returnsNull() {
        DeferredValue deferredValue = DeferredValue.withNullValue();
        assertNull(deferredValue.getSerializedValue(serializationService));
        assertNull(deferredValue.get(serializationService));
    }

    @Test
    public void testCopy_whenNullValue() {
        DeferredValue nullValue = DeferredValue.withNullValue();
        DeferredValue copy = nullValue.shallowCopy();
        assertNull(copy.getSerializedValue(serializationService));
        assertNull(copy.get(serializationService));
    }

    @Test
    public void testCopy_whenSerializedValue() {
        DeferredValue<String> v1 = withSerializedValue(serializedValue);
        DeferredValue<String> v2 = v1.shallowCopy();
        assertEquals(v1, v2);
    }

    @Test
    public void testCopy_whenValue() {
        DeferredValue<String> v1 = withValue(expected);
        DeferredValue<String> v2 = v1.shallowCopy();
        assertEquals(v1, v2);
    }

    @Test
    public void test_setOfValues() {
        assertTrue(deferredSet.contains(DeferredValue.withValue("1")));
        assertTrue(deferredSet.contains(DeferredValue.withValue("2")));
        assertTrue(deferredSet.contains(DeferredValue.withValue("3")));
        assertFalse(deferredSet.contains(DeferredValue.withValue("4")));
    }

    @Test
    public void test_adaptedSet() {
        assertTrue(adaptedSet.containsAll(valueSet));
        assertFalse(adaptedSet.isEmpty());
        assertEquals(3, adaptedSet.size());

        // add "4" -> "1", "2", "3", "4"
        adaptedSet.add("4");
        assertTrue(deferredSet.contains(DeferredValue.withValue("4")));

        // remove "1" -> "2", "3", "4"
        adaptedSet.remove("1");
        assertFalse(deferredSet.contains(DeferredValue.withValue("1")));

        // retain just "2" & "3"
        adaptedSet.retainAll(valueSet);
        assertEquals(2, adaptedSet.size());
        List<String> currentContents = Arrays.asList("2", "3");
        assertTrue(adaptedSet.containsAll(currentContents));
        assertTrue(adaptedSet.contains("2"));

        // toArray
        Object[] valuesAsArray = adaptedSet.toArray();
        assertEquals(2, valuesAsArray.length);
        assertArrayEquals(new Object[] {"2", "3"}, valuesAsArray);
        String[] stringArray = adaptedSet.toArray(new String[0]);
        assertEquals(2, stringArray.length);
        assertArrayEquals(new Object[] {"2", "3"}, stringArray);

        // iterator
        Iterator<String> iterator = adaptedSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("2", iterator.next());
        // iterator.remove -> just "3" left in set
        iterator.remove();
        assertEquals(1, adaptedSet.size());
        assertEquals("3", deferredSet.iterator().next().get(serializationService));

        // removeAll
        adaptedSet.removeAll(valueSet);
        assertTrue(adaptedSet.isEmpty());

        // addAll
        adaptedSet.addAll(valueSet);
        assertEquals(3, adaptedSet.size());
        assertTrue(adaptedSet.containsAll(valueSet));

        // clear
        adaptedSet.clear();
        assertTrue(adaptedSet.isEmpty());
        assertTrue(deferredSet.isEmpty());
    }
}
