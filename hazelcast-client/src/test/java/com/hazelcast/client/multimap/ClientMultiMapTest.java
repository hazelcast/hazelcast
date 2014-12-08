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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
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

    static HazelcastInstance server;
    static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPut() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
    }

    @Test(expected = NullPointerException.class)
    public void testPut_withNullValue() {
        Object key ="key";
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put(key, null);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_withNullKey() {
        Object value ="value";
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put(null, value);
    }

    @Test
    public void testPutMultiValuesToKey() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key, 1);
        assertTrue(mm.put(key, 2));
    }

    @Test
    public void testPut_WithExistingKeyValue() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
        assertFalse(mm.put(key, 1));
    }

    @Test
    public void testValueCount() {
        final Object key = "key1";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key, 1);
        mm.put(key, 2);

        assertEquals(2, mm.valueCount(key));
    }

    @Test
    public void testValueCount_whenKeyNotThere() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertEquals(0, mm.valueCount("NOT_THERE"));
    }

    @Test
    public void testSizeCount() {
        final Object key1 = "key1";
        final Object key2 = "key2";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key1, 1);
        mm.put(key1, 2);

        mm.put(key2, 1);
        mm.put(key2, 2);
        mm.put(key2, 2);

        assertEquals(4, mm.size());
    }

    @Test
    public void testEmptySizeCount() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(0, mm.size());
    }

    @Test
    public void testGet_whenNotExist() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.get("NOT_THERE");

        assertEquals(Collections.EMPTY_SET, coll);
    }

    @Test
    public void testGet() {
        final Object key = "key";
        final int maxItemsPerKey = 33;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
            expected.add(i);
        }

        Collection resultSet = new TreeSet( mm.get(key) );

        assertEquals(expected, resultSet);
    }

    @Test
    public void testRemove_whenKeyNotExist() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.remove("NOT_THERE");

        assertEquals(Collections.EMPTY_SET, coll);
    }

    @Test
    public void testRemoveKey() {
        final Object key = "key";
        final int maxItemsPerKey = 44;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
            expeted.add(i);
        }
        Set resultSet  = new TreeSet( mm.remove(key) );

        assertEquals(expeted, resultSet);
        assertEquals(0, mm.size());
    }

    @Test
    public void testRemoveValue_whenValueNotExists() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
        }
        boolean result = mm.remove(key, "NOT_THERE");

        assertFalse(result);
    }

    @Test
    public void testRemoveKeyValue() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
        }

        for ( int i=0; i< maxItemsPerKey; i++ ){
            boolean result = mm.remove(key, i);
            assertTrue(result);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.localKeySet();
    }

    @Test
    public void testEmptyKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.keySet());
    }

    @Test
    public void testKeySet() {
        final int maxKeys = 23;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for ( int key=0; key< maxKeys; key++ ){
            mm.put(key, 1);
            expected.add(key);
        }

        assertEquals(expected, mm.keySet());
    }

    @Test
    public void testValues_whenEmptyCollection() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_LIST, mm.values());
    }

    @Test
    public void testKeyValues() {
        final int maxKeys = 31;
        final int maxValues = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for ( int key=0; key< maxKeys; key++ ){
            for ( int val=0; val< maxValues; val++ ){
                mm.put(key, val);
                expected.add(val);
            }
        }

        Set resultSet = new TreeSet( mm.values() );

        assertEquals(expected, resultSet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.entrySet());
    }

    @Test
    public void testEntrySet() {
        final int maxKeys = 14;
        final int maxValues = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int key=0; key< maxKeys; key++ ){
            for ( int val=0; val< maxValues; val++ ){
                mm.put(key, val);
            }
        }

        assertEquals(maxKeys * maxValues, mm.entrySet().size());
    }

    @Test
    public void testContainsKey_whenKeyExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsKey("key1"));
    }

    @Test
    public void testContainsKey_whenKeyNotExists() {
        final MultiMap mm = client.getMultiMap(randomString());

        assertFalse(mm.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        final MultiMap mm = client.getMultiMap(randomString());

        mm.containsKey(null);
    }

    @Test
    public void testContainsValue_whenExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsValue("value1"));
        assertFalse(mm.containsValue("NOT_THERE"));
    }

    @Test
    public void testContainsValue_whenNotExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertFalse(mm.containsValue("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenSearchValueNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.containsValue(null);
    }

    @Test
    public void testContainsEntry() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsEntry("key1", "value1"));
        assertFalse(mm.containsEntry("key1", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "value1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalMultiMapStats() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.getLocalMultiMapStats();
    }

    @Test
    public void testClear() {
        final MultiMap mm = client.getMultiMap(randomString());
        final int maxKeys = 9;
        final int maxValues = 3;

        for ( int key=0; key< maxKeys; key++ ){
            for ( int val=0; val< maxValues; val++ ){
                mm.put(key, val);
            }
        }
        mm.clear();
        assertEquals(0, mm.size());
    }
}
