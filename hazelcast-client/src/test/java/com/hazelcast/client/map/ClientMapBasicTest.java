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
import com.hazelcast.map.MapEvent;
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

    static HazelcastInstance client;
    static HazelcastInstance server;

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
    public void testClientGetMap() {
        assertNotNull( client.getMap(randomString()) );
    }

    @Test
    public void testGetName() {
        String mapName = randomString();
        final IMap map = client.getMap(mapName);
        assertEquals(mapName, map.getName());
    }

    @Test
    public void testSize_whenEmpty() {
        final IMap map = client.getMap(randomString());
        assertEquals(0, map.size());
    }

    @Test
    public void testSize() {
        final IMap map = client.getMap(randomString());
        map.put("key", "val");
        assertEquals(1, map.size());
    }

    @Test
    public void testSize_withMultiKeyPuts() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        map.put(key, newValue);

        assertEquals(1, map.size());
    }

    @Test
    public void testIsEmpty_whenEmpty() {
        final IMap map = client.getMap(randomString());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        final IMap map = client.getMap(randomString());
        map.put("key", "val");
        assertFalse(map.isEmpty());
    }

    @Test
    public void testIsEmpty_afterPutRemove() {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        map.put(key, "val");
        map.remove(key);
        assertTrue(map.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        final Object val = "Val";
        map.put(null, val);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPut_whenValueNull() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        map.put(key, null);
    }

    @Test
    public void testPut() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Val";

        final Object result = map.put(key, value);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPut_whenKeyExists() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        final Object result = map.put(key, newValue);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        final Object result = map.put(key, value, 5, TimeUnit.MINUTES);
        assertNull(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTTL_whenKeyExists() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        final Object result = map.put(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpire() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        final Object result = map.put(key, value, 1, TimeUnit.SECONDS);
        assertNull(result);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutTTL_AfterExpireWhenKeyExists() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        final Object result = map.put(key, newValue, 1, TimeUnit.SECONDS);
        assertEquals(oldValue, result);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAsync() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Val";

        Future result = map.putAsync(key, value);

        assertEquals(null, result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsync_whenKeyExists() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object val = "Val";

        map.putAsync(null, val);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPutAsync_withValueNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "key";

        map.putAsync(key, null);
    }

    @Test
    public void testPutAsyncTTL() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Val";

        Future result = map.putAsync(key, value, 5, TimeUnit.MINUTES);

        assertEquals(null, result.get());
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_whenKeyExists() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(oldValue, result.get());
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpire() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Val";

        Future result = map.putAsync(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAsyncTTL_afterExpireWhenKeyExists() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        Future result = map.putAsync(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(oldValue, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testTryPut_whenNotLocked() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        boolean result = map.tryPut(key, value, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testTryPut_whenKeyPresentAndNotLocked() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "Val";

        map.put(key, oldValue);
        boolean result = map.tryPut(key, newValue, 1, TimeUnit.SECONDS);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object value = "Value";
        map.putIfAbsent(null, value);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testPutIfAbsent_whenValueNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        map.putIfAbsent(key, null);
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        Object result = map.putIfAbsent(key, value);

        assertEquals(null, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsent_whenKeyPresent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        map.put(key, value);
        Object result = map.putIfAbsent(key, value);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValue_whenKeyPresent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";
        final Object newValue = "newValue";

        map.put(key, value);
        Object result = map.putIfAbsent(key, newValue);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        Object result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertEquals(null, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenExpire() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        final Object result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertEquals(null, result);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresentAfterExpire() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        map.put(key, value);
        final Object result = map.putIfAbsent(key, value, 1, TimeUnit.SECONDS);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        map.put(key, value);
        final Object result = map.putIfAbsent(key, value, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutIfAbsentNewValueTTL_whenKeyPresent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";
        final Object newValue = "newValue";

        map.put(key, value);
        final Object result = map.putIfAbsent(key, newValue, 5, TimeUnit.MINUTES);

        assertEquals(value, result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testClear_whenEmpty() throws Exception {
        final IMap map = client.getMap(randomString());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        map.put(key, value);
        map.clear();

        assertTrue(map.isEmpty());
    }

    @Test
    public void testContainsKey_whenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        assertFalse(map.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        map.containsKey(null);
    }

    @Test
    public void testContainsKey_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        map.put(key, "val");
        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue_whenValueAbsent() {
        final IMap map = client.getMap(randomString());
        assertFalse(map.containsValue("NOT_THERE"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testContainsValue_whenValueNull() {
        final IMap map = client.getMap(randomString());
        map.containsValue(null);
    }

    @Test
    public void testContainsValue_whenValuePresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        final Object value = "value";
        map.put(key, value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testContainsValue_whenMultiValuePresent() {
        final IMap map = client.getMap(randomString());
        final Object value = "value";
        map.put("key1", value);
        map.put("key2", value);
        assertTrue(map.containsValue(value));
    }

    @Test
    public void testGet_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.put(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testGet_whenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        assertEquals(null, map.get("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        map.get(null);
    }

    @Test
    public void testGetAsync_whenKeyPresent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.put(key, val);
        Future result = map.getAsync(key);
        assertEquals(val, result.get());
    }

    @Test
    public void testGetAsync_whenKeyAbsent() throws Exception {
        final IMap map = client.getMap(randomString());

        Future result = map.getAsync("NOT_THERE");
        assertEquals(null, result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync_whenKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());
        map.getAsync(null);
    }

    @Test
    public void testMapSet() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.set(key, val);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSet_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "Val";
        final Object newValue = "newValue";

        map.set(key, oldValue);
        map.set(key, newValue);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testMapSetTTl() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.set(key, val, 5, TimeUnit.MINUTES);
        assertEquals(val, map.get(key));
    }

    @Test
    public void testMapSetTTl_whenExpired() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.set(key, val, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testMapSetTTl_whenReplacingKeyAndExpired() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object newValue = "newValue";
        final Object oldValue = "oldvalue";

        map.set(key, oldValue);
        map.set(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testRemove_WhenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        assertNull(map.remove("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_WhenKeyNull() {
        final IMap map = client.getMap(randomString());
        assertNull(map.remove(null));
    }

    @Test
    public void testRemove_WhenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertEquals(value, map.remove(key));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertTrue(map.remove(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenValueAbsent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertFalse(map.remove(key, "NOT_THERE"));
        assertEquals(value, map.get(key));
    }

    @Test
    public void testRemoveKeyValue_WhenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertFalse(map.remove("NOT_THERE", value));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        Future result = map.removeAsync(key);

        assertEquals(value, result.get());
        assertEquals(null, map.get(key));
    }

    @Test
    public void testRemoveAsync_whenKeyNotPresent() throws Exception {
        final IMap map = client.getMap(randomString());

        Future result = map.removeAsync("NOT_THERE");
        assertEquals(null, result.get());
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync_whenKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());
        map.removeAsync(null);
    }

    @Test
    public void testTryRemove_WhenKeyPresentAndNotLocked() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertTrue(result);
        assertNull(map.get(key));
    }

    @Test
    public void testTryRemove_WhenKeyAbsentAndNotLocked() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        boolean result = map.tryRemove(key, 1, TimeUnit.SECONDS);
        assertFalse(result);
    }


    @Test(expected = NullPointerException.class)
    public void testDelete_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        map.delete(null);
    }

    @Test
    public void testDelete_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        map.put(key, value);

        map.delete(key);
        assertEquals(0, map.size());
    }

    @Test
    public void testDelete_whenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        map.put(key, value);

        map.delete("NOT_THERE");
        assertEquals(1, map.size());
    }

    @Test
    public void testEvict_whenKeyAbsent() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        boolean result = map.evict("NOT_THERE");
        assertFalse( result );
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testEvict_whenKeyNull() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        map.evict(null);
    }

    @Test
    public void testEvict() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        boolean result = map.evict(key);
        assertTrue(result);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutAll() {
        final int max = 100;
        final IMap map = client.getMap(randomString());

        final Map expected = new HashMap();
        for (int i = 0; i < max; i++) {
            expected.put(i, i);
        }
        map.putAll(expected);

        for(Object key : expected.keySet()){
            Object value = map.get(key);
            Object expectedValue = expected.get(key);
            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testGetAll() {
        final int max = 100;
        final IMap map = client.getMap(randomString());

        final Map expected = new HashMap();
        for (int i = 0; i < max; i++) {
            map.put(i, i);
            expected.put(i, i);
        }
        Map result = map.getAll(expected.keySet());

        for(Object key : expected.keySet()){
            Object value = result.get(key);
            Object expectedValue = expected.get(key);
            assertEquals(expectedValue, value);
        }
    }

    public void testGetAll_whenMapEmpty() {
        final int max = 10;
        final IMap map = client.getMap(randomString());
        final Map expected = new HashMap();
        for (int i = 0; i < max; i++) {
            expected.put(i, i);
        }

        Map result = map.getAll(expected.keySet());

        assertTrue(result.isEmpty());
    }

    @Test
    public void testReplace_whenKeyValueAbsent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        assertNull(map.replace(key, value));
        assertNull(map.get(key));
    }

    @Test
    public void testReplace() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "value";
        final Object newValue = "NewValue";

        map.put(key, oldValue);
        final Object result = map.replace(key, newValue);

        assertEquals(oldValue, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        final Object newValue = "NewValue";

        map.put(key, value);
        final boolean result = map.replace(key, value, newValue);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue_whenValueAbsent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        final Object newValue = "NewValue";

        map.put(key, value);
        final boolean result = map.replace(key, "NOT_THERE", newValue);

        assertFalse(result);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.putTransient(key, value, 5, TimeUnit.MINUTES);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testPutTransient_whenExpire() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.putTransient(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresent() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 5, TimeUnit.MINUTES);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testPutTransient_whenKeyPresentAfterExpire() throws InterruptedException {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object oldValue = "oldValue";
        final Object newValue = "newValue";

        map.put(key, oldValue);
        map.putTransient(key, newValue, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, map.get(key));
    }

    @Test
    public void testGetEntryView_whenKeyAbsent(){
        final IMap map = client.getMap(randomString());
        final EntryView view = map.getEntryView("NOT_THERE");

        assertEquals(null, view);
    }

    @Test
    public void testGetEntryView(){
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "Value";

        map.put(key, value);
        final EntryView view = map.getEntryView(key);


        assertEquals(key, view.getKey());
        assertEquals(value, view.getValue());
    }

    @Test
    public void testKeySet_whenEmpty() {
        final IMap map = client.getMap(randomString());
        final Set keySet = map.keySet();
        assertTrue(keySet.isEmpty());
    }

    @Test
    public void testKeySet() {
        final int max = 81;
        final IMap map = client.getMap(randomString());

        final Set expected = new TreeSet();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            expected.add(key);
            map.put(key, value);
        }
        final Set keySet = map.keySet();

        assertEquals(expected, keySet);
    }

    @Test
    public void testKeySet_withPredicate() {
        final int max = 44;
        final IMap map = client.getMap(randomString());

        final Set expected = new TreeSet();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            map.put(key, value);
        }
        expected.add(4);

        final Set keySet = map.keySet(new SqlPredicate("this == 4value"));

        assertEquals(expected, keySet);
    }

    @Test
    public void testValues_whenEmpty() {
        final IMap map = client.getMap(randomString());
        final Collection values = map.values();
        assertTrue(values.isEmpty());
    }

    @Test
    public void testValues() {
        final int max = 23;
        final IMap map = client.getMap(randomString());

        final Set expected = new TreeSet();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            expected.add(value);
            map.put(key, value);
        }
        final Collection collection = map.values();
        final Set resultSet = new TreeSet(collection);

        assertEquals(expected, resultSet);
    }

    @Test
    public void testValues_withPredicate() {
        final int max = 27;
        final IMap map = client.getMap(randomString());

        final Set expected = new TreeSet();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            map.put(key, value);
        }
        expected.add(4);

        final Set keySet = map.keySet(new SqlPredicate("this == 4value"));

        assertEquals(expected, keySet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        final IMap map = client.getMap(randomString());
        Set<Map.Entry> entrySet = map.entrySet();
        assertTrue(entrySet.isEmpty());
    }

    @Test
    public void testEntrySet() {
        final int max = 34;
        final IMap map = client.getMap(randomString());

        final Map expected = new HashMap();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            expected.put(key, value);
            map.put(key, value);
        }
        Set<Map.Entry> entrySet = map.entrySet();

        for(Map.Entry entry : entrySet){
            Object value = entry.getValue();
            Object key = entry.getKey();
            Object expectedValue = expected.get(key);

            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testEntrySet_withPredicate() {
        final int max = 44;
        final IMap map = client.getMap(randomString());

        final Map expected = new HashMap();
        for (int key = 0; key < max; key++) {
            Object value = key+"value";
            expected.put(key, value);
            map.put(key, value);
        }

        final Set<Map.Entry> entrySet = map.entrySet(new SqlPredicate("this == 1value"));

        Map.Entry entry = entrySet.iterator().next();
        assertEquals(1, entry.getKey());
        assertEquals("1value", entry.getValue());
        assertEquals(1, entrySet.size());
    }

    @Test
    public void testMapStatistics_withClientOperations() {
        final String mapName = randomString();
        final LocalMapStats serverMapStats = server.getMap(mapName).getLocalMapStats();

        final IMap map = client.getMap(mapName);
        final int operationCount = 1123;
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
    public void testAddLocalEntryListener(){
        final IMap map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicate(){
        final IMap map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener(), new DumPredicate(), true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicateAndKey(){
        final IMap map = client.getMap(randomString());
        map.addLocalEntryListener(new DumEntryListener(), new DumPredicate(), "Key", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet(){
        final IMap map = client.getMap(randomString());
        map.localKeySet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet_WithPredicate(){
        final IMap map = client.getMap(randomString());
        map.localKeySet(new DumPredicate());
    }

    static class DumEntryListener implements EntryListener {
        public void entryAdded(EntryEvent event) {
        }
        public void entryRemoved(EntryEvent event) {
        }
        public void entryUpdated(EntryEvent event) {
        }
        public void entryEvicted(EntryEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
            // TODO what to do here?
        }
    }

    static class DumPredicate implements Predicate {
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }
}
