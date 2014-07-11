/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.multimap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapTest {

    private static HazelcastInstance client;

    private Object key;
    private MultiMap<Object, Object> multiMap;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        key = randomString();
        multiMap = client.getMultiMap(randomString());
    }

    @Test
    public void testPut() {
        assertTrue(multiMap.put(key, 1));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPut_withNullValue() {
        assertFalse(multiMap.put(key, null));
    }

    @Test(expected = NullPointerException.class)
    public void testPut_withNullKey() {
        assertFalse(multiMap.put(null, randomString()));
    }

    @Test
    public void testPutMultiValuesToKey() {
        multiMap.put(key, 1);
        assertTrue(multiMap.put(key, 2));
    }

    @Test
    public void testPut_WithExistingKeyValue() {
        assertTrue(multiMap.put(key, 1));
        assertFalse(multiMap.put(key, 1));
    }

    @Test
    public void testValueCount() {
        multiMap.put(key, 1);
        multiMap.put(key, 2);

        assertEquals(2, multiMap.valueCount(key));
    }

    @Test
    public void testValueCount_whenKeyNotThere() {
        assertEquals(0, multiMap.valueCount("NOT_THERE"));
    }

    @Test
    public void testSizeCount() {
        multiMap.put(key, 1);
        multiMap.put(key, 2);

        Object anotherKey = randomString();
        multiMap.put(anotherKey, 1);
        multiMap.put(anotherKey, 2);
        multiMap.put(anotherKey, 2);

        assertEquals(4, multiMap.size());
    }

    @Test
    public void testEmptySizeCount() {
        assertEquals(0, multiMap.size());
    }

    @Test
    public void testGet_whenNotExist() {
        Collection<Object> coll = multiMap.get("NOT_THERE");

        assertEquals(Collections.EMPTY_LIST, coll);
    }

    @Test
    public void testGet() {
        int maxItemsPerKey = 33;

        Set<Object> expected = new TreeSet<Object>();
        for (int i = 0; i < maxItemsPerKey; i++) {
            multiMap.put(key, i);
            expected.add(i);
        }

        Collection<Object> resultSet = new TreeSet<Object>(multiMap.get(key));

        assertEquals(expected, resultSet);
    }

    @Test
    public void testRemove_whenKeyNotExist() {
        Collection<Object> coll = multiMap.remove("NOT_THERE");

        assertEquals(Collections.EMPTY_LIST, coll);
    }

    @Test
    public void testRemoveKey() {
        int maxItemsPerKey = 44;

        Set<Object> expected = new TreeSet<Object>();
        for (int i = 0; i < maxItemsPerKey; i++) {
            multiMap.put(key, i);
            expected.add(i);
        }
        Set resultSet = new TreeSet<Object>(multiMap.remove(key));

        assertEquals(expected, resultSet);
        assertEquals(0, multiMap.size());
    }

    @Test
    public void testRemoveValue_whenValueNotExists() {
        int maxItemsPerKey = 4;

        for (int i = 0; i < maxItemsPerKey; i++) {
            multiMap.put(key, i);
        }
        boolean result = multiMap.remove(key, "NOT_THERE");

        assertFalse(result);
    }

    @Test
    public void testRemoveKeyValue() {
        int maxItemsPerKey = 4;

        for (int i = 0; i < maxItemsPerKey; i++) {
            multiMap.put(key, i);
        }

        for (int i = 0; i < maxItemsPerKey; i++) {
            boolean result = multiMap.remove(key, i);
            assertTrue(result);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        multiMap.localKeySet();
    }

    @Test
    public void testEmptyKeySet() {
        assertEquals(Collections.EMPTY_SET, multiMap.keySet());
    }

    @Test
    public void testKeySet() {
        int maxKeys = 23;

        Set<Object> expected = new TreeSet<Object>();
        for (int key = 0; key < maxKeys; key++) {
            multiMap.put(key, 1);
            expected.add(key);
        }

        assertEquals(expected, multiMap.keySet());
    }

    @Test
    public void testValues_whenEmptyCollection() {
        assertEquals(Collections.EMPTY_LIST, multiMap.values());
    }

    @Test
    public void testKeyValues() {
        int maxKeys = 31;
        int maxValues = 3;

        Set<Object> expected = new TreeSet<Object>();
        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                multiMap.put(key, val);
                expected.add(val);
            }
        }

        Set<Object> resultSet = new TreeSet<Object>(multiMap.values());

        assertEquals(expected, resultSet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        assertEquals(Collections.EMPTY_SET, multiMap.entrySet());
    }

    @Test
    public void testEnterySet() {
        int maxKeys = 14;
        int maxValues = 3;

        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                multiMap.put(key, val);
            }
        }

        assertEquals(maxKeys * maxValues, multiMap.entrySet().size());
    }

    @Test
    public void testContainsKey_whenKeyExists() {
        multiMap.put(key, "value1");

        assertTrue(multiMap.containsKey(key));
    }

    @Test
    public void testContainsKey_whenKeyNotExists() {
        assertFalse(multiMap.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        assertFalse(multiMap.containsKey(null));
    }

    @Test
    public void testContainsValue_whenExists() {
        multiMap.put(key, "value1");

        assertTrue(multiMap.containsValue("value1"));
        assertFalse(multiMap.containsValue("NOT_THERE"));
    }

    @Test
    public void testContainsValue_whenNotExists() {
        assertFalse(multiMap.containsValue("NOT_THERE"));
    }

    @Test
    public void testContainsValue_whenSearchValueNull() {
        assertFalse(multiMap.containsValue(null));
    }

    @Test
    public void testContainsEntry() {
        multiMap.put(key, "value1");

        assertTrue(multiMap.containsEntry(key, "value1"));
        assertFalse(multiMap.containsEntry(key, "NOT_THERE"));
        assertFalse(multiMap.containsEntry("NOT_THERE", "NOT_THERE"));
        assertFalse(multiMap.containsEntry("NOT_THERE", "value1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalMultiMapStats() {
        multiMap.getLocalMultiMapStats();
    }

    @Test
    public void testClear() {
        int maxKeys = 9;
        int maxValues = 3;

        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                multiMap.put(key, val);
            }
        }
        multiMap.clear();

        assertEquals(0, multiMap.size());
    }
}