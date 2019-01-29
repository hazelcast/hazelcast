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

package com.hazelcast.client.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapBasicTest extends AbstractClientMapTest {

    @Test
    public void testClientGetMap() {
        assertNotNull(client.getMap(randomString()));
    }

    @Test
    public void testGetName() {
        String mapName = randomString();
        IMap<String, String> map = client.getMap(mapName);
        assertEquals(mapName, map.getName());
    }

    @Test
    public void testSize_whenEmpty() {
        IMap<String, String> map = client.getMap(randomString());
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
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        map.put(key, newValue);

        assertEquals(1, map.size());
    }

    @Test
    public void testIsEmpty_whenEmpty() {
        IMap<String, String> map = client.getMap(randomString());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        IMap<String, String> map = client.getMap(randomString());
        map.put("key", "val");
        assertFalse(map.isEmpty());
    }

    @Test
    public void testIsEmpty_afterPutRemove() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";
        map.put(key, "val");
        map.remove(key);
        assertTrue(map.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenKeyNull() {
        IMap<String, String> map = client.getMap(randomString());
        String val = "Val";
        map.put(null, val);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenValueNull() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        map.put(key, null);
    }

    @Test
    public void testPut() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        String result = map.put(key, value);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPut_whenKeyExists() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        String result = map.put(key, newValue);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        String result = map.put(key, value, 5, TimeUnit.MINUTES);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTTL_whenKeyExists() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        String result = map.put(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpire() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        String result = map.put(key, value, 1, TimeUnit.SECONDS);
        assertNull(result);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpireWhenKeyExists() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        String result = map.put(key, newValue, 1, TimeUnit.SECONDS);
        assertEquals(oldValue, result);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testPutAsync() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        Future result = map.putAsync(key, value);

        assertNull(result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsync_whenKeyExists() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withKeyNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String val = "Val";

        map.putAsync(null, val);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withValueNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";

        map.putAsync(key, null);
    }

    @Test
    public void testPutAsyncTTL() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        Future result = map.putAsync(key, value, 5, TimeUnit.MINUTES);

        assertNull(result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_whenKeyExists() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpire() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        Future result = map.putAsync(key, value, 1, TimeUnit.SECONDS);
        assertNull(result.get());
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpireWhenKeyExists() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 1, TimeUnit.SECONDS);

        assertEquals(oldValue, result.get());
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testSetAsync() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        Future<Void> result = map.setAsync(key, value);

        result.get();
        assertEquals(value, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testSetAsync_withKeyNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String val = "Val";

        map.setAsync(null, val);
    }

    @Test(expected = NullPointerException.class)
    public void testSetAsync_withValueNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";

        map.setAsync(key, null);
    }

    @Test
    public void testSetAsyncTTL() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";

        Future<Void> result = map.setAsync(key, value, 5, TimeUnit.MINUTES);

        result.get();
        assertEquals(value, map.get(key));
    }

    @Test
    public void testSetAsyncTTL_afterExpire() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Val";
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryEvictedListener<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<Void> result = map.setAsync(key, value, 1, TimeUnit.SECONDS);
        result.get();
        assertOpenEventually(latch);
        assertNull(map.get(key));
    }

    @Test
    public void testSetAsyncTTL_afterExpireWhenKeyExists() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryEvictedListener<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        map.set(key, oldValue);
        Future<Void> result = map.setAsync(key, newValue, 1, TimeUnit.SECONDS);
        result.get();
        assertOpenEventually(latch);
        assertNull(map.get(key));
    }

    @Test
    public void testTryPut_whenNotLocked() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        boolean result = map.tryPut(key, value, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testTryPut_whenKeyPresentAndNotLocked() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "Val";

        map.put(key, oldValue);
        boolean result = map.tryPut(key, newValue, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenKeyNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String value = "Value";
        map.putIfAbsent(null, value);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenValueNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";
        map.putIfAbsent(key, null);
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        String result = map.putIfAbsent(key, value);

        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsent_whenKeyPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        map.put(key, value);
        String result = map.putIfAbsent(key, value);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValue_whenKeyPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";
        String newValue = "newValue";

        map.put(key, value);
        String result = map.putIfAbsent(key, newValue);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        String result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenExpire() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        String result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertNull(result);
        assertNull(map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresentAfterExpire() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        map.put(key, value);
        String result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        map.put(key, value);
        String result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValueTTL_whenKeyPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";
        String newValue = "newValue";

        map.put(key, value);
        String result = map.putIfAbsent(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testClear_whenEmpty() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        map.put(key, value);
        map.clear();

        assertTrue(map.isEmpty());
    }

    @Test
    public void testContainsKey_whenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        assertFalse(map.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        IMap<String, String> map = client.getMap(randomString());
        map.containsKey(null);
    }

    @Test
    public void testContainsKey_whenKeyPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";
        map.put(key, "val");
        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_whenValueAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        assertFalse(map.containsValue("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenValueNull() {
        IMap<String, String> map = client.getMap(randomString());
        map.containsValue(null);
    }

    @Test
    public void testContainsValue_whenValuePresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "key";
        String value = "value";
        map.put(key, value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testContainsValue_whenMultiValuePresent() {
        IMap<String, String> map = client.getMap(randomString());
        String value = "value";
        map.put("key1", value);
        map.put("key2", value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testGet_whenKeyPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String val = "Val";

        map.put(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testGet_whenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        assertNull(map.get("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        IMap<String, String> map = client.getMap(randomString());
        map.get(null);
    }

    @Test
    public void testGetAsync_whenKeyPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String val = "Val";

        map.put(key, val);
        Future result = map.getAsync(key);
        assertEquals(val, result.get());
    }

    @Test
    public void testGetAsync_whenKeyAbsent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());

        Future result = map.getAsync("NOT_THERE");
        assertNull(result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync_whenKeyNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        map.getAsync(null);
    }

    @Test
    public void testMapSet() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String val = "Val";

        map.set(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSet_whenKeyPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "Val";
        String newValue = "newValue";

        map.set(key, oldValue);
        map.set(key, newValue);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testMapSetTTl() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String val = "Val";

        map.set(key, val, 5, TimeUnit.MINUTES);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSetTTl_whenExpired() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String val = "Val";

        map.set(key, val, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testMapSetTTl_whenReplacingKeyAndExpired() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String newValue = "newValue";
        String oldValue = "oldvalue";

        map.set(key, oldValue);
        map.set(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testRemove_WhenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        assertNull(map.remove("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_WhenKeyNull() {
        IMap<String, String> map = client.getMap(randomString());
        assertNull(map.remove(null));
    }

    @Test
    public void testRemove_WhenKeyPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        assertEquals(value, map.remove(key));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        assertTrue(map.remove(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenValueAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        assertFalse(map.remove(key, "NOT_THERE"));
        assertEquals(value, map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        assertFalse(map.remove("NOT_THERE", value));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        Future result = map.removeAsync(key);

        assertEquals(value, result.get());
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveAsync_whenKeyNotPresent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());

        Future result = map.removeAsync("NOT_THERE");
        assertNull(result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync_whenKeyNull() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        map.removeAsync(null);
    }

    @Test
    public void testTryRemove_WhenKeyPresentAndNotLocked() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertTrue(result);
        assertNull(map.get(key));
    }

    @Test
    public void testTryRemove_WhenKeyAbsentAndNotLocked() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";

        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete_whenKeyNull() {
        IMap<String, String> map = client.getMap(randomString());
        map.delete(null);
    }

    @Test
    public void testDelete_whenKeyPresent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";
        map.put(key, value);

        map.delete(key);
        assertEquals(0, map.size());
    }

    @Test
    public void testDelete_whenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";
        map.put(key, value);

        map.delete("NOT_THERE");
        assertEquals(1, map.size());
    }

    @Test
    public void testEvict_whenKeyAbsent() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        boolean result = map.evict("NOT_THERE");
        assertFalse(result);
    }

    @Test(expected = NullPointerException.class)
    public void testEvict_whenKeyNull() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        map.evict(null);
    }

    @Test
    public void testEvict() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.put(key, value);
        boolean result = map.evict(key);
        assertTrue(result);
        assertNull(map.get(key));
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

        assertEquals(max, map.size());
        for (Integer key : expected.keySet()) {
            Integer value = map.get(key);
            Integer expectedValue = expected.get(key);
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

        for (Integer key : expected.keySet()) {
            Integer value = result.get(key);
            Integer expectedValue = expected.get(key);
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
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        assertNull(map.replace(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testReplace() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "value";
        String newValue = "NewValue";

        map.put(key, oldValue);
        String result = map.replace(key, newValue);

        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";
        String newValue = "NewValue";

        map.put(key, value);
        boolean result = map.replace(key, value, newValue);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue_whenValueAbsent() throws Exception {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";
        String newValue = "NewValue";

        map.put(key, value);
        boolean result = map.replace(key, "NOT_THERE", newValue);

        assertFalse(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.putTransient(key, value, 5, TimeUnit.MINUTES);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient_whenExpire() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "value";

        map.putTransient(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresent() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresentAfterExpire() throws InterruptedException {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String oldValue = "oldValue";
        String newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertNull(map.get(key));
    }

    @Test
    public void testGetEntryView_whenKeyAbsent() {
        IMap<String, String> map = client.getMap(randomString());
        EntryView view = map.getEntryView("NOT_THERE");

        assertNull(view);
    }

    @Test
    public void testGetEntryView() {
        IMap<String, String> map = client.getMap(randomString());
        String key = "Key";
        String value = "Value";

        map.put(key, value);
        EntryView view = map.getEntryView(key);

        assertEquals(key, view.getKey());
        assertEquals(value, view.getValue());
    }

    @Test
    public void testKeySet_whenEmpty() {
        IMap<String, String> map = client.getMap(randomString());
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
        IMap<String, String> map = client.getMap(randomString());
        Collection values = map.values();
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

        Set<String> expected = new TreeSet<String>();
        for (int key = 0; key < max; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        expected.add("4value");

        Collection<String> collection = map.values(new SqlPredicate("this == 4value"));
        Set<String> resultSet = new TreeSet<String>(collection);

        assertEquals(expected, resultSet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        IMap<String, String> map = client.getMap(randomString());
        Set<Map.Entry<String, String>> entrySet = map.entrySet();
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
            Integer key = entry.getKey();
            String value = entry.getValue();
            String expectedValue = expected.get(key);

            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void github_11489_verifyNoFailingCastOnValue() throws Exception {
        IMap<Integer, Integer> test = client.getMap("github_11489");
        for (int i = 0; i < 1000; i++) {
            test.put(i, i);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String result = objectMapper.writeValueAsString(test.values(new TestPagingPredicate(1000)));
        assertNotNull(result);
    }

    private static class TestPagingPredicate extends PagingPredicate {

        public TestPagingPredicate(int pageSize) {
            super(pageSize);
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return true;
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
        assertEquals(1, entrySet.size());

        Map.Entry<Integer, String> entry = entrySet.iterator().next();
        assertEquals(1, (int) entry.getKey());
        assertEquals("1value", entry.getValue());
    }

    @Test
    public void testMapStatistics_withClientOperations() {
        String mapName = randomString();
        IMap<Integer, Integer> member1Map = member1.getMap(mapName);
        member1Map.addInterceptor(new DelayGetRemoveMapInterceptor());
        LocalMapStats stats1 = member1Map.getLocalMapStats();
        LocalMapStats stats2 = member2.getMap(mapName).getLocalMapStats();

        IMap<Integer, Integer> map = client.getMap(mapName);
        int operationCount = 1123;
        for (int i = 0; i < operationCount; i++) {
            map.put(i, i);
            map.get(i);
            map.remove(i);
        }

        assertEquals("put count: stats1" + stats1 + " stats2:" + stats2, operationCount,
                stats1.getPutOperationCount() + stats2.getPutOperationCount());
        assertEquals("get count : stats1" + stats1 + " stats2:" + stats2, operationCount,
                stats1.getGetOperationCount() + stats2.getGetOperationCount());
        assertEquals("remove count : stats1" + stats1 + " stats2:" + stats2, operationCount,
                stats1.getRemoveOperationCount() + stats2.getRemoveOperationCount());
        assertTrue("put latency : stats1" + stats1 + " stats2:" + stats2,
                0 < stats1.getTotalPutLatency() + stats2.getTotalPutLatency());
        assertTrue("get latency : stats1" + stats1 + " stats2:" + stats2,
                0 < stats1.getTotalGetLatency() + stats2.getTotalGetLatency());
        assertTrue("remove latency : stats1" + stats1 + " stats2:" + stats2,
                0 < stats1.getTotalRemoveLatency() + stats2.getTotalRemoveLatency());
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("deprecation")
    public void testAddLocalEntryListener() {
        IMap<String, String> map = client.getMap(randomString());
        map.addLocalEntryListener(new EmptyEntryListener());
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("deprecation")
    public void testAddLocalEntryListener_WithPredicate() {
        IMap<String, String> map = client.getMap(randomString());
        map.addLocalEntryListener(new EmptyEntryListener(), new FalsePredicate(), true);
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("deprecation")
    public void testAddLocalEntryListener_WithPredicateAndKey() {
        IMap<String, String> map = client.getMap(randomString());
        map.addLocalEntryListener(new EmptyEntryListener(), new FalsePredicate(), "Key", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        IMap<String, String> map = client.getMap(randomString());
        map.localKeySet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet_WithPredicate() {
        IMap<String, String> map = client.getMap(randomString());
        map.localKeySet(new FalsePredicate());
    }

    private static class DelayGetRemoveMapInterceptor implements MapInterceptor, Serializable {

        @Override
        public Object interceptGet(Object value) {
            sleepMillis(1);
            return value;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            sleepMillis(1);
            return newValue;
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            sleepMillis(1);
            return removedValue;
        }

        @Override
        public void afterRemove(Object value) {
        }
    }

    private static class EmptyEntryListener implements EntryListener<String, String> {

        public void entryAdded(EntryEvent event) {
        }

        public void entryRemoved(EntryEvent event) {
        }

        public void entryUpdated(EntryEvent event) {
        }

        public void entryEvicted(EntryEvent event) {
        }

        public void mapEvicted(MapEvent event) {
        }

        public void mapCleared(MapEvent event) {
        }
    }

    private static class FalsePredicate implements Predicate<String, String> {

        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }
}
