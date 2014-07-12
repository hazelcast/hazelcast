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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapBasicTest {

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientGetMap() {
        assertNotNull(client.getMap(randomString()));
    }

    @Test
    public void testGetName() {
        String mapName = randomString();
        IMap<Object, Object> map = client.getMap(mapName);
        assertEquals(mapName, map.getName());
    }

    @Test
    public void testSize_whenEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertEquals(0, map.size());
    }

    @Test
    public void testSize() {
        IMap<String, String> map = client.getMap(randomString());
        map.put("key", "val");
        assertEquals(1, map.size());
    }

    @Test
    public void testSize_withMultiKeyPuts() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        map.put(key, newValue);

        assertEquals(1, map.size());
    }

    @Test
    public void testIsEmpty_whenEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.put("key", "val");
        assertFalse(map.isEmpty());
    }

    @Test
    public void testIsEmpty_afterPutRemove() {
        IMap<Object, String> map = client.getMap(randomString());
        Object key = "key";
        map.put(key, "val");
        map.remove(key);
        assertTrue(map.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenKeyNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object val = "Val";
        map.put(null, val);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPut_whenValueNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        map.put(key, null);
    }

    @Test
    public void testPut() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Val";

        Object result = map.put(key, value);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPut_whenKeyExists() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Object result = map.put(key, newValue);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        Object result = map.put(key, value, 5, TimeUnit.MINUTES);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTTL_whenKeyExists() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Object result = map.put(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpire() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        Object result = map.put(key, value, 1, TimeUnit.SECONDS);
        assertNull(result);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpireWhenKeyExists() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Object result = map.put(key, newValue, 1, TimeUnit.SECONDS);
        assertEquals(oldValue, result);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAsync() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Val";

        Future result = map.putAsync(key, value);

        assertEquals(null, result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsync_whenKeyExists() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withKeyNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object val = "Val";

        map.putAsync(null, val);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPutAsync_withValueNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "key";

        map.putAsync(key, null);
    }

    @Test
    public void testPutAsyncTTL() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Val";

        Future result = map.putAsync(key, value, 5, TimeUnit.MINUTES);

        assertEquals(null, result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_whenKeyExists() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpire() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Val";

        Future result = map.putAsync(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpireWhenKeyExists() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(oldValue, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testTryPut_whenNotLocked() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        boolean result = map.tryPut(key, value, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testTryPut_whenKeyPresentAndNotLocked() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "Val";

        map.put(key, oldValue);
        boolean result = map.tryPut(key, newValue, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenKeyNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object value = "Value";
        map.putIfAbsent(null, value);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPutIfAbsent_whenValueNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "key";
        map.putIfAbsent(key, null);
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        Object result = map.putIfAbsent(key, value);

        assertEquals(null, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsent_whenKeyPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        map.put(key, value);
        Object result = map.putIfAbsent(key, value);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValue_whenKeyPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";
        Object newValue = "newValue";

        map.put(key, value);
        Object result = map.putIfAbsent(key, newValue);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        Object result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertEquals(null, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenExpire() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        Object result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertEquals(null, result);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresentAfterExpire() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        map.put(key, value);
        Object result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        map.put(key, value);
        Object result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValueTTL_whenKeyPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";
        Object newValue = "newValue";

        map.put(key, value);
        Object result = map.putIfAbsent(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testClear_whenEmpty() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        map.put(key, value);
        map.clear();

        assertTrue(map.isEmpty());
    }

    @Test
    public void testContainsKey_whenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertFalse(map.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.containsKey(null);
    }

    @Test
    public void testContainsKey_whenKeyPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "key";
        map.put(key, "val");
        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_whenValueAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertFalse(map.containsValue("NOT_THERE"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testContainsValue_whenValueNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.containsValue(null);
    }

    @Test
    public void testContainsValue_whenValuePresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "key";
        Object value = "value";
        map.put(key, value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testContainsValue_whenMultiValuePresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object value = "value";
        map.put("key1", value);
        map.put("key2", value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testGet_whenKeyPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object val = "Val";

        map.put(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testGet_whenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertEquals(null, map.get("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.get(null);
    }

    @Test
    public void testGetAsync_whenKeyPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object val = "Val";

        map.put(key, val);
        Future result = map.getAsync(key);
        assertEquals(val, result.get());
    }

    @Test
    public void testGetAsync_whenKeyAbsent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());

        Future result = map.getAsync("NOT_THERE");
        assertEquals(null, result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync_whenKeyNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        map.getAsync(null);
    }

    @Test
    public void testMapSet() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object val = "Val";

        map.set(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSet_whenKeyPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "Val";
        Object newValue = "newValue";

        map.set(key, oldValue);
        map.set(key, newValue);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testMapSetTTl() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object val = "Val";

        map.set(key, val, 5, TimeUnit.MINUTES);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSetTTl_whenExpired() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object val = "Val";

        map.set(key, val, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testMapSetTTl_whenReplacingKeyAndExpired() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object newValue = "newValue";
        Object oldValue = "oldvalue";

        map.set(key, oldValue);
        map.set(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testRemove_WhenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertNull(map.remove("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_WhenKeyNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        assertNull(map.remove(null));
    }

    @Test
    public void testRemove_WhenKeyPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        assertEquals(value, map.remove(key));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        assertTrue(map.remove(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenValueAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        assertFalse(map.remove(key, "NOT_THERE"));
        assertEquals(value, map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        assertFalse(map.remove("NOT_THERE", value));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        Future result = map.removeAsync(key);

        assertEquals(value, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testRemoveAsync_whenKeyNotPresent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());

        Future result = map.removeAsync("NOT_THERE");
        assertEquals(null, result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync_whenKeyNull() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        map.removeAsync(null);
    }

    @Test
    public void testTryRemove_WhenKeyPresentAndNotLocked() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertTrue(result);
        assertNull(map.get(key));
    }

    @Test
    public void testTryRemove_WhenKeyAbsentAndNotLocked() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";

        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete_whenKeyNull() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.delete(null);
    }

    @Test
    public void testDelete_whenKeyPresent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";
        map.put(key, value);

        map.delete(key);
        assertEquals(0, map.size());
    }

    @Test
    public void testDelete_whenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";
        map.put(key, value);

        map.delete("NOT_THERE");
        assertEquals(1, map.size());
    }

    @Test
    public void testEvict_whenKeyAbsent() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        boolean result = map.evict("NOT_THERE");
        assertFalse(result);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testEvict_whenKeyNull() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        map.evict(null);
    }

    @Test
    public void testEvict() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.put(key, value);
        boolean result = map.evict(key);
        assertTrue(result);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAll() {
        int max = 100;
        IMap<Integer, Integer> map = client.getMap(randomString());

        Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
        for (int i = 0; i < max; i++) {
            expected.put(i, i);
        }
        map.putAll(expected);

        for (int key : expected.keySet()) {
            int value = map.get(key);
            int expectedValue = expected.get(key);
            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testGetAll() {
        int max = 100;
        IMap<Integer, Integer> map = client.getMap(randomString());

        Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
        for (int i = 0; i < max; i++) {
            map.put(i, i);
            expected.put(i, i);
        }
        Map<Integer, Integer> result = map.getAll(expected.keySet());

        for (int key : expected.keySet()) {
            int value = result.get(key);
            int expectedValue = expected.get(key);
            assertEquals(expectedValue, value);
        }
    }

    public void testGetAll_whenMapEmpty() {
        int max = 10;
        IMap<Integer, Integer> map = client.getMap(randomString());

        Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
        for (int i = 0; i < max; i++) {
            expected.put(i, i);
        }

        Map<Integer, Integer> result = map.getAll(expected.keySet());

        assertTrue(result.isEmpty());
    }

    @Test
    public void testReplace_whenKeyValueAbsent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        assertNull(map.replace(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testReplace() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "value";
        Object newValue = "NewValue";

        map.put(key, oldValue);
        Object result = map.replace(key, newValue);

        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";
        Object newValue = "NewValue";

        map.put(key, value);
        boolean result = map.replace(key, value, newValue);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue_whenValueAbsent() throws Exception {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";
        Object newValue = "NewValue";

        map.put(key, value);
        boolean result = map.replace(key, "NOT_THERE", newValue);

        assertFalse(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.putTransient(key, value, 5, TimeUnit.MINUTES);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient_whenExpire() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "value";

        map.putTransient(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresent() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresentAfterExpire() throws InterruptedException {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object oldValue = "oldValue";
        Object newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testGetEntryView_whenKeyAbsent() {
        IMap<Object, Object> map = client.getMap(randomString());
        EntryView view = map.getEntryView("NOT_THERE");

        assertEquals(null, view);
    }

    @Test
    public void testGetEntryView() {
        IMap<Object, Object> map = client.getMap(randomString());
        Object key = "Key";
        Object value = "Value";

        map.put(key, value);
        EntryView view = map.getEntryView(key);

        assertEquals(key, view.getKey());
        assertEquals(value, view.getValue());
    }

    @Test
    public void testKeySet_whenEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());
        Set keySet = map.keySet();
        assertTrue(keySet.isEmpty());
    }

    @Test
    public void testKeySet() {
        int max = 81;
        IMap<Integer, String> map = client.getMap(randomString());

        Set<Integer> expected = new TreeSet<Integer>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            expected.add(key);
            map.put(key, value);
        }
        Set<Integer> keySet = map.keySet();

        assertEquals(expected, keySet);
    }

    @Test
    public void testKeySet_withPredicate() {
        int max = 44;
        IMap<Integer, String> map = client.getMap(randomString());

        Set<Integer> expected = new TreeSet<Integer>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        expected.add(4);

        Set<Integer> keySet = map.keySet(new SqlPredicate("this == 4value"));

        assertEquals(expected, keySet);
    }

    @Test
    public void testValues_whenEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());

        Collection<Object> values = map.values();
        assertTrue(values.isEmpty());
    }

    @Test
    public void testValues() {
        int max = 23;
        IMap<Integer, String> map = client.getMap(randomString());

        Set<String> expected = new TreeSet<String>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            expected.add(value);
            map.put(key, value);
        }
        Collection<String> collection = map.values();
        Set<String> resultSet = new TreeSet<String>(collection);

        assertEquals(expected, resultSet);
    }

    @Test
    public void testValues_withPredicate() {
        int max = 27;
        IMap<Integer, String> map = client.getMap(randomString());

        Set<Integer> expected = new TreeSet<Integer>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        expected.add(4);

        Set<Integer> keySet = map.keySet(new SqlPredicate("this == 4value"));

        assertEquals(expected, keySet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        IMap<Object, Object> map = client.getMap(randomString());
        Set<Map.Entry<Object, Object>> entrySet = map.entrySet();
        assertTrue(entrySet.isEmpty());
    }

    @Test
    public void testEntrySet() {
        int max = 34;
        IMap<Integer, String> map = client.getMap(randomString());

        Map<Integer, String> expected = new HashMap<Integer, String>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            expected.put(key, value);
            map.put(key, value);
        }
        Set<Map.Entry<Integer, String>> entrySet = map.entrySet();

        for (Map.Entry<Integer, String> entry : entrySet) {
            int key = entry.getKey();
            String value = entry.getValue();
            String expectedValue = expected.get(key);

            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testEntrySet_withPredicate() {
        int max = 44;
        IMap<Integer, String> map = client.getMap(randomString());

        for (int key = 0; key < max; key++) {
            String value = key + "value";
            map.put(key, value);
        }

        Set<Map.Entry<Integer, String>> entrySet = map.entrySet(new SqlPredicate("this == 1value"));

        Map.Entry<Integer, String> entry = entrySet.iterator().next();
        assertEquals(1, (int) entry.getKey());
        assertEquals("1value", entry.getValue());
        assertEquals(1, entrySet.size());
    }

    @Test
    public void testMapStatistics_withClientOperations() {
        String mapName = randomString();
        LocalMapStats serverMapStats = server.getMap(mapName).getLocalMapStats();

        IMap<Object, Object> map = client.getMap(mapName);
        int operationCount = 1123;
        for (int i = 0; i < operationCount; i++) {
            map.put(i, i);
            map.get(i);
            map.remove(i);
        }

        assertEquals("put count", operationCount, serverMapStats.getPutOperationCount());
        assertEquals("get count", operationCount, serverMapStats.getGetOperationCount());
        assertEquals("remove count", operationCount, serverMapStats.getRemoveOperationCount());
        assertTrue("put latency", 0 < serverMapStats.getTotalPutLatency());
        assertTrue("get latency", 0 < serverMapStats.getTotalGetLatency());
        assertTrue("remove latency", 0 < serverMapStats.getTotalRemoveLatency());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener<Object, Object>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicate() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener<Object, Object>(), new DumPredicate<Object, Object>(), true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicateAndKey() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener<Object, Object>(), new DumPredicate<Object, Object>(), "Key", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.localKeySet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet_WithPredicate() {
        IMap<Object, Object> map = client.getMap(randomString());
        map.localKeySet(new DumPredicate());
    }

    private static class DumEntryListener<K, V> implements EntryListener<K, V> {
        public void entryAdded(EntryEvent<K, V> event) {
        }

        public void entryRemoved(EntryEvent<K, V> event) {
        }

        public void entryUpdated(EntryEvent<K, V> event) {
        }

        public void entryEvicted(EntryEvent<K, V> event) {
        }

        public void mapEvicted(MapEvent event) {
        }

        public void mapCleared(MapEvent event) {
        }
    }

    private static class DumPredicate<K, V> implements Predicate<K, V> {
        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }
}
