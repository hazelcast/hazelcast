/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
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

    private static final String KEY = "key";
    private static final String OLD_VALUE = "oldValue";
    private static final String NEW_VALUE = "newValue";

    private static HazelcastInstance client;
    private static HazelcastInstance server1;
    private static HazelcastInstance server2;
    private static HazelcastInstance server3;

    private String randomMapName = randomString();
    private IMap<String, String> randomStringMap = client.getMap(randomMapName);

    @BeforeClass
    public static void init() {
        server1 = Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();
        server3 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientGetMap() {
        assertNotNull(client.getMap(randomMapName));
    }

    @Test
    public void testGetName() {
        assertEquals(randomMapName, randomStringMap.getName());
    }

    @Test
    public void testSize_whenEmpty() {
        assertEquals(0, randomStringMap.size());
    }

    @Test
    public void testSize() {
        randomStringMap.put(KEY, NEW_VALUE);

        assertEquals(1, randomStringMap.size());
    }

    @Test
    public void testSize_withMultiKeyPuts() {
        randomStringMap.put(KEY, OLD_VALUE);
        randomStringMap.put(KEY, NEW_VALUE);

        assertEquals(1, randomStringMap.size());
    }

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(randomStringMap.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        randomStringMap.put(KEY, NEW_VALUE);

        assertFalse(randomStringMap.isEmpty());
    }

    @Test
    public void testIsEmpty_afterPutRemove() {
        randomStringMap.put(KEY, NEW_VALUE);
        randomStringMap.remove(KEY);

        assertTrue(randomStringMap.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenKeyNull() {
        randomStringMap.put(null, NEW_VALUE);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenValueNull() {
        randomStringMap.put(KEY, null);
    }

    @Test
    public void testPut() {
        String oldValue = randomStringMap.put(KEY, NEW_VALUE);

        assertNull(oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPut_whenKeyExists() {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.put(KEY, NEW_VALUE);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTTL() {
        String oldValue = randomStringMap.put(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertNull(oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTTL_whenKeyExists() {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.put(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTTL_AfterExpire() {
        String oldValue = randomStringMap.put(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        assertNull(oldValue);

        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTTL_AfterExpireWhenKeyExists() {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.put(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        assertEquals(OLD_VALUE, oldValue);

        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAsync() throws Exception {
        Future oldValue = randomStringMap.putAsync(KEY, NEW_VALUE);

        assertEquals(null, oldValue.get());
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAsync_whenKeyExists() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        Future oldValue = randomStringMap.putAsync(KEY, NEW_VALUE);

        assertEquals(OLD_VALUE, oldValue.get());
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withKeyNull() throws Exception {
        randomStringMap.putAsync(null, NEW_VALUE);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsync_withValueNull() throws Exception {
        randomStringMap.putAsync(KEY, null);
    }

    @Test
    public void testPutAsyncTTL() throws Exception {
        Future oldValue = randomStringMap.putAsync(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(null, oldValue.get());
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAsyncTTL_whenKeyExists() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        Future oldValueFuture = randomStringMap.putAsync(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(OLD_VALUE, oldValueFuture.get());
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAsyncTTL_afterExpire() throws Exception {
        Future oldValueFuture = randomStringMap.putAsync(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertEquals(null, oldValueFuture.get());
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAsyncTTL_afterExpireWhenKeyExists() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        Future oldValueFuture = randomStringMap.putAsync(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertEquals(OLD_VALUE, oldValueFuture.get());
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testTryPut_whenNotLocked() throws Exception {
        boolean putSuccess = randomStringMap.tryPut(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);

        assertTrue(putSuccess);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testTryPut_whenKeyPresentAndNotLocked() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        boolean putSuccess = randomStringMap.tryPut(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);

        assertTrue(putSuccess);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenKeyNull() throws Exception {
        randomStringMap.putIfAbsent(null, NEW_VALUE);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsent_whenValueNull() throws Exception {
        randomStringMap.putIfAbsent(KEY, null);
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE);

        assertEquals(null, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsent_whenKeyPresent() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.putIfAbsent(KEY, OLD_VALUE);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(OLD_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentNewValue_whenKeyPresent() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(OLD_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentTTL() throws Exception {
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(null, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentTTL_whenExpire() throws Exception {
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertEquals(null, oldValue);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresentAfterExpire() throws Exception {
        randomStringMap.put(KEY, NEW_VALUE);
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);

        assertEquals(NEW_VALUE, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentTTL_whenKeyPresent() throws Exception {
        randomStringMap.put(KEY, NEW_VALUE);
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(NEW_VALUE, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutIfAbsentNewValueTTL_whenKeyPresent() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.putIfAbsent(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(OLD_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testClear_whenEmpty() throws Exception {
        randomStringMap.clear();
        assertTrue(randomStringMap.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        randomStringMap.put(KEY, NEW_VALUE);
        randomStringMap.clear();

        assertTrue(randomStringMap.isEmpty());
    }

    @Test
    public void testContainsKey_whenKeyAbsent() {
        assertFalse(randomStringMap.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        randomStringMap.containsKey(null);
    }

    @Test
    public void testContainsKey_whenKeyPresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertTrue(randomStringMap.containsKey(KEY));
    }

    @Test
    public void testContainsValue_whenValueAbsent() {
        assertFalse(randomStringMap.containsValue("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenValueNull() {
        randomStringMap.containsValue(null);
    }

    @Test
    public void testContainsValue_whenValuePresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertTrue(randomStringMap.containsValue(NEW_VALUE));
    }

    @Test
    public void testContainsValue_whenMultiValuePresent() {
        randomStringMap.put("key1", NEW_VALUE);
        randomStringMap.put("key2", NEW_VALUE);
        assertTrue(randomStringMap.containsValue(NEW_VALUE));
    }

    @Test
    public void testGet_whenKeyPresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testGet_whenKeyAbsent() {
        assertEquals(null, randomStringMap.get("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenKeyNull() {
        randomStringMap.get(null);
    }

    @Test
    public void testGetAsync_whenKeyPresent() throws Exception {
        randomStringMap.put(KEY, NEW_VALUE);
        Future getFuture = randomStringMap.getAsync(KEY);
        assertEquals(NEW_VALUE, getFuture.get());
    }

    @Test
    public void testGetAsync_whenKeyAbsent() throws Exception {
        Future getFuture = randomStringMap.getAsync("NOT_THERE");
        assertEquals(null, getFuture.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync_whenKeyNull() throws Exception {
        randomStringMap.getAsync(null);
    }

    @Test
    public void testMapSet() {
        randomStringMap.set(KEY, NEW_VALUE);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testMapSet_whenKeyPresent() {
        randomStringMap.set(KEY, OLD_VALUE);
        randomStringMap.set(KEY, NEW_VALUE);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testMapSetTTl() {
        randomStringMap.set(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testMapSetTTl_whenExpired() {
        randomStringMap.set(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testMapSetTTl_whenReplacingKeyAndExpired() {
        randomStringMap.set(KEY, OLD_VALUE);
        randomStringMap.set(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testRemove_WhenKeyAbsent() {
        assertNull(randomStringMap.remove("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testRemove_WhenKeyNull() {
        assertNull(randomStringMap.remove(null));
    }

    @Test
    public void testRemove_WhenKeyPresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertEquals(NEW_VALUE, randomStringMap.remove(KEY));
        assertNull(randomStringMap.get(KEY));
    }

    @Test
    public void testRemoveKeyValue_WhenPresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertTrue(randomStringMap.remove(KEY, NEW_VALUE));
        assertNull(randomStringMap.get(KEY));
    }

    @Test
    public void testRemoveKeyValue_WhenValueAbsent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertFalse(randomStringMap.remove(KEY, "NOT_THERE"));
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testRemoveKeyValue_WhenKeyAbsent() {
        randomStringMap.put(KEY, NEW_VALUE);
        assertFalse(randomStringMap.remove("NOT_THERE", NEW_VALUE));
    }

    @Test
    public void testRemoveAsync() throws Exception {
        randomStringMap.put(KEY, NEW_VALUE);
        Future removeFuture = randomStringMap.removeAsync(KEY);

        assertEquals(NEW_VALUE, removeFuture.get());
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testRemoveAsync_whenKeyNotPresent() throws Exception {
        Future removeFuture = randomStringMap.removeAsync("NOT_THERE");
        assertEquals(null, removeFuture.get());
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync_whenKeyNull() throws Exception {
        randomStringMap.removeAsync(null);
    }

    @Test
    public void testTryRemove_WhenKeyPresentAndNotLocked() {
        randomStringMap.put(KEY, NEW_VALUE);
        boolean removeSuccess = randomStringMap.tryRemove(KEY, 1, TimeUnit.SECONDS);

        assertTrue(removeSuccess);
        assertNull(randomStringMap.get(KEY));
    }

    @Test
    public void testTryRemove_WhenKeyAbsentAndNotLocked() {
        boolean removeSuccess = randomStringMap.tryRemove(KEY, 1, TimeUnit.SECONDS);
        assertFalse(removeSuccess);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete_whenKeyNull() {
        randomStringMap.delete(null);
    }

    @Test
    public void testDelete_whenKeyPresent() {
        randomStringMap.put(KEY, NEW_VALUE);
        randomStringMap.delete(KEY);
        assertEquals(0, randomStringMap.size());
    }

    @Test
    public void testDelete_whenKeyAbsent() {
        randomStringMap.put(KEY, NEW_VALUE);
        randomStringMap.delete("NOT_THERE");
        assertEquals(1, randomStringMap.size());
    }

    @Test
    public void testEvict_whenKeyAbsent() throws InterruptedException {
        boolean evictSuccess = randomStringMap.evict("NOT_THERE");
        assertFalse(evictSuccess);
    }

    @Test(expected = NullPointerException.class)
    public void testEvict_whenKeyNull() throws InterruptedException {
        randomStringMap.evict(null);
    }

    @Test
    public void testEvict() throws InterruptedException {
        randomStringMap.put(KEY, NEW_VALUE);
        boolean evictSuccess = randomStringMap.evict(KEY);

        assertTrue(evictSuccess);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutAll() {
        int mapSize = 100;
        IMap<Integer, Integer> map = client.getMap(randomMapName);

        Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < mapSize; i++) {
            expectedMap.put(i, i);
        }
        map.putAll(expectedMap);

        for (Integer key : expectedMap.keySet()) {
            Integer value = map.get(key);
            Integer expectedValue = expectedMap.get(key);
            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testGetAll() {
        int mapSize = 100;
        IMap<Integer, Integer> map = client.getMap(randomMapName);

        Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
            expectedMap.put(i, i);
        }
        Map<Integer, Integer> getAllMap = map.getAll(expectedMap.keySet());

        for (Integer key : expectedMap.keySet()) {
            Integer value = getAllMap.get(key);
            Integer expectedValue = expectedMap.get(key);
            assertEquals(expectedValue, value);
        }
    }

    public void testGetAll_whenMapEmpty() {
        int mapSize = 10;
        IMap<Integer, Integer> map = client.getMap(randomMapName);

        Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < mapSize; i++) {
            expectedMap.put(i, i);
        }
        Map getAllMap = map.getAll(expectedMap.keySet());

        assertTrue(getAllMap.isEmpty());
    }

    @Test
    public void testReplace_whenKeyValueAbsent() throws Exception {
        assertNull(randomStringMap.replace(KEY, NEW_VALUE));
        assertNull(randomStringMap.get(KEY));
    }

    @Test
    public void testReplace() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        String oldValue = randomStringMap.replace(KEY, NEW_VALUE);

        assertEquals(OLD_VALUE, oldValue);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testReplaceKeyValue() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        boolean replaceSuccess = randomStringMap.replace(KEY, OLD_VALUE, NEW_VALUE);

        assertTrue(replaceSuccess);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testReplaceKeyValue_whenValueAbsent() throws Exception {
        randomStringMap.put(KEY, OLD_VALUE);
        boolean replaceSuccess = randomStringMap.replace(KEY, "NOT_THERE", NEW_VALUE);

        assertFalse(replaceSuccess);
        assertEquals(OLD_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTransient() throws InterruptedException {
        randomStringMap.putTransient(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTransient_whenExpire() throws InterruptedException {
        randomStringMap.putTransient(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTransient_whenKeyPresent() throws InterruptedException {
        randomStringMap.put(KEY, OLD_VALUE);
        randomStringMap.putTransient(KEY, NEW_VALUE, 5, TimeUnit.MINUTES);
        assertEquals(NEW_VALUE, randomStringMap.get(KEY));
    }

    @Test
    public void testPutTransient_whenKeyPresentAfterExpire() throws InterruptedException {
        randomStringMap.put(KEY, OLD_VALUE);
        randomStringMap.putTransient(KEY, NEW_VALUE, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(null, randomStringMap.get(KEY));
    }

    @Test
    public void testGetEntryView_whenKeyAbsent() {
        EntryView view = randomStringMap.getEntryView("NOT_THERE");
        assertEquals(null, view);
    }

    @Test
    public void testGetEntryView() {
        randomStringMap.put(KEY, NEW_VALUE);
        EntryView view = randomStringMap.getEntryView(KEY);

        assertEquals(KEY, view.getKey());
        assertEquals(NEW_VALUE, view.getValue());
    }

    @Test
    public void testKeySet_whenEmpty() {
        assertTrue(randomStringMap.keySet().isEmpty());
    }

    @Test
    public void testKeySet() {
        int mapSize = 81;
        IMap<Integer, String> map = client.getMap(randomMapName);

        Set<Integer> expectedSet = new TreeSet<Integer>();
        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            expectedSet.add(key);
            map.put(key, value);
        }
        Set<Integer> mapKeySet = map.keySet();

        assertEquals(mapSize, mapKeySet.size());
        assertEquals(expectedSet, mapKeySet);
    }

    @Test
    public void testKeySet_withPredicate() {
        int mapSize = 44;
        IMap<Integer, String> map = client.getMap(randomMapName);

        Set<Integer> expectedSet = new TreeSet<Integer>();
        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        expectedSet.add(4);
        Set<Integer> mapKeySet = map.keySet(new SqlPredicate("this == 4value"));

        assertEquals(expectedSet, mapKeySet);
    }

    @Test
    public void testValues_whenEmpty() {
        assertTrue(randomStringMap.values().isEmpty());
    }

    @Test
    public void testValues() {
        int mapSize = 23;
        IMap<Integer, String> map = client.getMap(randomMapName);

        Set<String> expectedSet = new TreeSet<String>();
        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            expectedSet.add(value);
            map.put(key, value);
        }

        Collection<String> mapValues = map.values();
        int size = mapValues.size();
        assertEquals("map.values().size() should be " + mapSize + ", but was " + size, mapSize, size);

        Set<String> malValuesSet = new TreeSet<String>(mapValues);
        assertEquals(expectedSet, malValuesSet);
    }

    @Test
    public void testValues_withPredicate() {
        int mapSize = 27;
        IMap<Integer, String> map = client.getMap(randomMapName);

        Set<String> expectedSet = new TreeSet<String>();
        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        expectedSet.add("4value");

        Set<String> mapValuesSet = new TreeSet<String>(map.values(new SqlPredicate("this == 4value")));
        assertEquals(expectedSet, mapValuesSet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        assertTrue(randomStringMap.entrySet().isEmpty());
    }

    @Test
    public void testEntrySet() {
        int mapSize = 34;
        IMap<Integer, String> map = client.getMap(randomMapName);

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();
        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            expectedMap.put(key, value);
            map.put(key, value);
        }
        Set<Map.Entry<Integer, String>> mapEntrySet = map.entrySet();

        for (Map.Entry<Integer, String> entry : mapEntrySet) {
            Integer key = entry.getKey();
            String value = entry.getValue();
            String expectedValue = expectedMap.get(key);

            assertEquals(expectedValue, value);
        }
    }

    @Test
    public void testEntrySet_withPredicate() {
        int mapSize = 44;
        IMap<Integer, String> map = client.getMap(randomMapName);

        for (int key = 0; key < mapSize; key++) {
            String value = key + "value";
            map.put(key, value);
        }
        Set<Map.Entry<Integer, String>> mapEntrySet = map.entrySet(new SqlPredicate("this == 1value"));

        Map.Entry entry = mapEntrySet.iterator().next();
        assertEquals(1, entry.getKey());
        assertEquals("1value", entry.getValue());
        assertEquals(1, mapEntrySet.size());
    }

    @Test
    public void testMapStatistics_withClientOperations() {
        int operationCount = 1123;
        IMap<Integer, Integer> map = client.getMap(randomMapName);

        for (int i = 0; i < operationCount; i++) {
            map.put(i, i);
            map.get(i);
            map.remove(i);
        }
        HashMap<String, Long> stats = new HashMap<String, Long>(3);
        addLocalMapStats(stats, server1.getMap(randomMapName).getLocalMapStats());
        addLocalMapStats(stats, server2.getMap(randomMapName).getLocalMapStats());
        addLocalMapStats(stats, server3.getMap(randomMapName).getLocalMapStats());

        assertEquals("put count", operationCount, (long) stats.get("put"));
        assertEquals("get count", operationCount, (long) stats.get("get"));
        assertEquals("remove count", operationCount, (long) stats.get("remove"));
        assertTrue("put latency", 0 < stats.get("putLatency"));
        assertTrue("get latency", 0 < stats.get("getLatency"));
        assertTrue("remove latency", 0 < stats.get("removeLatency"));
    }

    private void addLocalMapStats(HashMap<String, Long> stats, LocalMapStats serverMapStats) {
        addValueToHashMap(stats, "put", serverMapStats.getPutOperationCount());
        addValueToHashMap(stats, "get", serverMapStats.getGetOperationCount());
        addValueToHashMap(stats, "remove", serverMapStats.getRemoveOperationCount());
        addValueToHashMap(stats, "putLatency", serverMapStats.getTotalPutLatency());
        addValueToHashMap(stats, "getLatency", serverMapStats.getTotalGetLatency());
        addValueToHashMap(stats, "removeLatency", serverMapStats.getTotalRemoveLatency());
    }

    private void addValueToHashMap(HashMap<String, Long> stats, String key, Long value) {
        Long oldValue = stats.get(key);
        stats.put(key, (oldValue == null) ? value : oldValue + value);
    }
}
