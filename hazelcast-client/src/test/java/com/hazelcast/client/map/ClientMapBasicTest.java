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

import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapBasicTest {

    static HazelcastInstance client;
    static HazelcastInstance server;

    static TestMapStore flushMapStore = new TestMapStore();
    static TestMapStore transientMapStore = new TestMapStore();

    @BeforeClass
    public static void init() {
        Config config = new Config();
        config.getMapConfig("flushMap").
                setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1000)
                        .setImplementation(flushMapStore));
        config.getMapConfig("putTransientMap").
                setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1000)
                        .setImplementation(transientMapStore));

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient(null);
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
    public void testIsEmpty_whenEmpty() {
        final IMap map = client.getMap(randomString());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        final IMap map = client.getMap(randomString());
        map.put("key", "val");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIsEmpty_afterPutRemove() {
        final IMap map = client.getMap(randomString());
        final Object key = "key";
        map.put(key, "val");
        map.remove(key);
        assertTrue(map.isEmpty());
    }

    @Test
    public void testPut() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        final Object result = map.put(key, val);
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPut_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        final Object val = "Val";

        final Object result = map.put(null, val);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPut_whenValueNull() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";

        final Object result = map.put(key, null);
    }

    @Test
    public void testPut_whenKeyExists() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";
        final Object expected = "expected";

        map.put(key, val);
        final Object result = map.put(key, expected);
        assertEquals(expected, result);
    }

    @Test
    public void testPutAsync() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        Future result = map.putAsync(key, val);

        assertEquals(val, result.get());
        assertEquals(val, map.get(key));
    }

    @Test
    public void testPutAsync_withKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object val = "Val";

        Future result = map.putAsync(null, val);
        result.get();
    }

    @Test
    public void testPutAsync_withValueNull() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "key";

        Future result = map.putAsync(key, null);
        result.get();
    }

    @Test
    public void testContains_whenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        assertFalse(map.containsKey("NOT_THERE"));
    }

    @Test
    public void testContains_whenKeyNull() {
        final IMap map = client.getMap(randomString());
        assertFalse(map.containsKey(null));
    }

    @Test
    public void testContains_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        Object key = "key";
        map.put(key, "val");
        assertTrue(map.containsKey(key));
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

    @Test
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

    @Test
    public void testGetAsync_whenKeyNull() throws Exception {
        final IMap map = client.getMap(randomString());

        Future result = map.getAsync(null);
    }

    @Test
    public void testMapSet_whenKeyPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";
        final Object expected = "expected";

        map.set(key, val);
        map.set(key, expected);
        assertEquals(expected, map.get(key));
    }

    @Test
    public void testMapSetTTl() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object val = "Val";

        map.set(key, val, 5, TimeUnit.SECONDS);
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
        final Object value = "value";
        final Object expected = "expected";

        map.set(key, expected);
        map.set(key, value, 1, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals(expected, map.get(key));
    }

    @Test
    public void testRemove_WhenKeyAbsent() {
        final IMap map = client.getMap(randomString());
        assertNull(map.remove("NOT_THERE"));
    }

    @Test
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
    }

    @Test
    public void testRemoveKeyValue_WhenPresent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertTrue(map.remove(key, value));
    }

    @Test
    public void testRemoveKeyValue_WhenValueAbsent() {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        map.put(key, value);
        assertFalse(map.remove(key, "NOT_THERE"));
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
    public void testPutAll() {
        final int max = 100;
        final IMap map = client.getMap(randomString());

        Map expected = new HashMap();
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

        Map expected = new HashMap();
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

    @Test
    public void testReplace_whenKeyValueAbsent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";

        assertNull(map.replace(key, value));
        assertEquals(value, map.get(key));
    }

    @Test
    public void testReplace() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        map.put(key, value);

        final Object newValue = "NewValue";
        final Object result = map.replace(key, newValue);

        assertEquals(value, result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        map.put(key, value);

        final Object newValue = "NewValue";
        final boolean result = map.replace(key, value, newValue);

        assertTrue(result);
        assertEquals(newValue, map.get(key));
    }

    @Test
    public void testReplaceKeyValue_whenValueAbsent() throws Exception {
        final IMap map = client.getMap(randomString());
        final Object key = "Key";
        final Object value = "value";
        map.put(key, value);

        final Object newValue = "NewValue";
        final Object result = map.replace(key, "NOT_THERE", newValue);

        assertEquals(null, result);
        assertEquals(value, map.get(key));
    }






    /*
    @Test
    public void testFlush() throws InterruptedException {
        flushMapStore.latch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap("flushMap");
        map.put(1l, "value");
        map.flush();
        assertOpenEventually(flushMapStore.latch, 5);
    }






    @Test
    public void testAsyncPutWithTtl() throws Exception {
        final IMap map = createMap();
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<String> f1 = map.putAsync("key", "value1", 3, TimeUnit.SECONDS);
        String f1Val = f1.get();
        assertNull(f1Val);
        assertEquals("value1", map.get("key"));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get("key"));
    }

    @Test
    public void testAsyncRemove() throws Exception {
        final IMap map = createMap();
        fillMap(map);
        Future f = map.removeAsync("key4");
        Object o = f.get();
        assertEquals("value4", o);
        assertEquals(9, map.size());
    }











    @Test
    public void testTryPutRemove() throws Exception {
        final IMap map = createMap();
        assertTrue(map.tryPut("key1", "value1", 1, TimeUnit.SECONDS));
        assertTrue(map.tryPut("key2", "value2", 1, TimeUnit.SECONDS));
        map.lock("key1");
        map.lock("key2");

        final CountDownLatch latch = new CountDownLatch(2);

        new Thread() {
            public void run() {
                boolean result = map.tryPut("key1", "value3", 1, TimeUnit.SECONDS);
                if (!result) {
                    latch.countDown();
                }
            }
        }.start();

        new Thread() {
            public void run() {
                boolean result = map.tryRemove("key2", 1, TimeUnit.SECONDS);
                if (!result) {
                    latch.countDown();
                }
            }
        }.start();

        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        map.forceUnlock("key1");
        map.forceUnlock("key2");

    }



    @Test
    public void testPutTtl() throws Exception {
        final IMap map = createMap();
        map.put("key1", "value1", 1, TimeUnit.SECONDS);
        assertNotNull(map.get("key1"));
        Thread.sleep(2000);
        assertNull(map.get("key1"));
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        final IMap map = createMap();
        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.putIfAbsent("key1", "value3"));
    }

    @Test
    public void testPutIfAbsentTtl() throws Exception {
        final IMap map = createMap();
        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
        assertNull(map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
    }


    @Test
    public void testPutTransient() throws InterruptedException {
        transientMapStore.latch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap("putTransientMap");
        map.putTransient(3l, "value1", 100, TimeUnit.SECONDS);
        map.flush();
        assertFalse(transientMapStore.latch.await(5, TimeUnit.SECONDS));
    }














    @Test
    public void testLock() throws Exception {
        final IMap map = createMap();
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.tryPut("key1", "value2", 1, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("value1", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testLockTtl() throws Exception {
        final IMap map = createMap();
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1", 2, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.tryPut("key1", "value2", 5, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
        assertEquals("value2", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testLockTtl2() throws Exception {
        final IMap map = createMap();
        map.lock("key1", 3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!map.tryLock("key1")) {
                    latch.countDown();
                }
                try {
                    if (map.tryLock("key1", 5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        map.forceUnlock("key1");
    }

    @Test
    public void testTryLock() throws Exception {
        final IMap map = createMap();
        final IMap tempMap = map;

        assertTrue(tempMap.tryLock("key1", 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!tempMap.tryLock("key1", 2, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(tempMap.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (tempMap.tryLock("key1", 20, TimeUnit.SECONDS)) {
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        tempMap.unlock("key1");
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(tempMap.isLocked("key1"));
        tempMap.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        final IMap map = createMap();
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.forceUnlock("key1");
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
    }









    @Test
    public void testValues() {
        final IMap map = createMap();
        fillMap(map);

        final Collection values = map.values(new SqlPredicate("this == value1"));
        assertEquals(1, values.size());
        assertEquals("value1", values.iterator().next());
    }












    @Test
    public void testSubmitToKey() throws Exception {
        final IMap map = createMap();
        map.put(1, 1);
        Future f = map.submitToKey(1, new IncrementorEntryProcessor());
        assertEquals(2, f.get());
        assertEquals(2, map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        final IMap map = createMap();
        Future f = map.submitToKey(11, new IncrementorEntryProcessor());
        assertEquals(1, f.get());
        assertEquals(1, map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        final IMap map = createMap();
        map.put(1, 1);
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutionCallback executionCallback = new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        map.submitToKey(1, new IncrementorEntryProcessor(), executionCallback);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, map.get(1));
    }









    @Test
    public void testListener() throws InterruptedException {
        final IMap map = createMap();
        final CountDownLatch latch1Add = new CountDownLatch(5);
        final CountDownLatch latch1Remove = new CountDownLatch(2);

        final CountDownLatch latch2Add = new CountDownLatch(1);
        final CountDownLatch latch2Remove = new CountDownLatch(1);

        EntryListener listener1 = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch1Add.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                latch1Remove.countDown();
            }
        };

        EntryListener listener2 = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch2Add.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                latch2Remove.countDown();
            }
        };

        map.addEntryListener(listener1, false);
        map.addEntryListener(listener2, "key3", true);

        Thread.sleep(1000);

        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");
        map.put("key5", "value5");

        map.remove("key1");
        map.remove("key3");

        assertTrue(latch1Add.await(10, TimeUnit.SECONDS));
        assertTrue(latch1Remove.await(10, TimeUnit.SECONDS));
        assertTrue(latch2Add.await(5, TimeUnit.SECONDS));
        assertTrue(latch2Remove.await(5, TimeUnit.SECONDS));
    }



    @Test
    public void testPredicateListenerWithPortableKey() throws InterruptedException {
        final IMap tradeMap = createMap();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        EntryListener listener = new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                atomicInteger.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        AuthenticationRequest key = new AuthenticationRequest(new UsernamePasswordCredentials("a", "b"));
        tradeMap.addEntryListener(listener, key, true);
        AuthenticationRequest key2 = new AuthenticationRequest(new UsernamePasswordCredentials("a", "c"));
        tradeMap.put(key2, 1);


        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(0, atomicInteger.get());
    }

    @Test
    public void testBasicPredicate() {
        final IMap map = createMap();
        fillMap(map);
        final Collection collection = map.values(new SqlPredicate("this == value1"));
        assertEquals("value1", collection.iterator().next());
        final Set set = map.keySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set.iterator().next());
        final Set<Map.Entry<String, String>> set1 = map.entrySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set1.iterator().next().getKey());
        assertEquals("value1", set1.iterator().next().getValue());
    }


*/




    /**
     * Issue #923
     */
    @Test
    public void testPartitionAwareKey() {
        String name = randomString();
        PartitionAwareKey key = new PartitionAwareKey("key", "123");
        String value = "value";

        IMap<Object, Object> map1 = server.getMap(name);
        map1.put(key, value);
        assertEquals(value, map1.get(key));

        IMap<Object, Object> map2 = client.getMap(name);
        assertEquals(value, map2.get(key));
    }

    private static class PartitionAwareKey implements PartitionAware, Serializable {
        private final String key;
        private final String pk;

        private PartitionAwareKey(String key, String pk) {
            this.key = key;
            this.pk = pk;
        }

        @Override
        public Object getPartitionKey() {
            return pk;
        }
    }

    @Test
    public void testExecuteOnKeys() throws Exception {
        String name = randomString();
        IMap<Integer, Integer> map = client.getMap(name);
        IMap<Integer, Integer> map2 = client.getMap(name);

        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }
        Set keys = new HashSet();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);
        final Map<Integer, Object> resultMap = map2.executeOnKeys(keys, new IncrementorEntryProcessor());
        assertEquals(1, resultMap.get(1));
        assertEquals(1, resultMap.get(4));
        assertEquals(1, resultMap.get(7));
        assertEquals(1, resultMap.get(9));
        assertEquals(1, (int) map.get(1));
        assertEquals(0, (int) map.get(2));
        assertEquals(0, (int) map.get(3));
        assertEquals(1, (int) map.get(4));
        assertEquals(0, (int) map.get(5));
        assertEquals(0, (int) map.get(6));
        assertEquals(1, (int) map.get(7));
        assertEquals(0, (int) map.get(8));
        assertEquals(1, (int) map.get(9));
    }

    /**
     * Issue #996
     */
    @Test
    public void testEntryListener() throws InterruptedException {
        final CountDownLatch gateAdd = new CountDownLatch(2);
        final CountDownLatch gateRemove = new CountDownLatch(1);
        final CountDownLatch gateEvict = new CountDownLatch(1);
        final CountDownLatch gateUpdate = new CountDownLatch(1);

        final String mapName = randomString();

        final IMap<Object, Object> serverMap = server.getMap(mapName);
        serverMap.put(3, new Deal(3));

        final IMap<Object, Object> clientMap = client.getMap(mapName);

        assertEquals(1, clientMap.size());

        final EntryListener listener = new EntListener(gateAdd, gateRemove, gateEvict, gateUpdate);

        clientMap.addEntryListener(listener, new SqlPredicate("id=1"), 2, true);
        clientMap.put(2, new Deal(1));
        clientMap.put(2, new Deal(1));
        clientMap.remove(2);

        clientMap.put(2, new Deal(1));
        clientMap.evict(2);

        assertTrue(gateAdd.await(10, TimeUnit.SECONDS));
        assertTrue(gateRemove.await(10, TimeUnit.SECONDS));
        assertTrue(gateEvict.await(10, TimeUnit.SECONDS));
        assertTrue(gateUpdate.await(10, TimeUnit.SECONDS));
    }

    static class EntListener implements EntryListener<Integer, Deal>, Serializable {
        private final CountDownLatch _gateAdd;
        private final CountDownLatch _gateRemove;
        private final CountDownLatch _gateEvict;
        private final CountDownLatch _gateUpdate;

        EntListener(CountDownLatch gateAdd, CountDownLatch gateRemove, CountDownLatch gateEvict, CountDownLatch gateUpdate) {
            _gateAdd = gateAdd;
            _gateRemove = gateRemove;
            _gateEvict = gateEvict;
            _gateUpdate = gateUpdate;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Deal> arg0) {
            _gateAdd.countDown();
        }

        @Override
        public void entryEvicted(EntryEvent<Integer, Deal> arg0) {
            _gateEvict.countDown();
        }

        @Override
        public void entryRemoved(EntryEvent<Integer, Deal> arg0) {
            _gateRemove.countDown();
        }

        @Override
        public void entryUpdated(EntryEvent<Integer, Deal> arg0) {
            _gateUpdate.countDown();
        }
    }

    static class Deal implements Serializable {
        Integer id;

        Deal(Integer id) {
            this.id = id;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }
    }

    private static class IncrementorEntryProcessor extends AbstractEntryProcessor implements DataSerializable {
        IncrementorEntryProcessor() {
            super(true);
        }

        public Object process(Map.Entry entry) {
            Integer value = (Integer) entry.getValue();
            if (value == null) {
                value = 0;
            }
            if (value == -1) {
                entry.setValue(null);
                return null;
            }
            value++;
            entry.setValue(value);
            return value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
        }
    }

    @Test
    public void testMapStatistics() throws Exception {
        String name = randomString();
        final LocalMapStats localMapStats = server.getMap(name).getLocalMapStats();
        final IMap map = client.getMap(name);

        final int operationCount = 1000;
        for (int i = 0; i < operationCount; i++) {
            map.put(i, i);
            map.get(i);
            map.remove(i);
        }

        assertEquals("put count", operationCount, localMapStats.getPutOperationCount());
        assertEquals("get count", operationCount, localMapStats.getGetOperationCount());
        assertEquals("remove count", operationCount, localMapStats.getRemoveOperationCount());
        assertTrue("put latency", 0 < localMapStats.getTotalPutLatency());
        assertTrue("get latency", 0 < localMapStats.getTotalGetLatency());
        assertTrue("remove latency", 0 < localMapStats.getTotalRemoveLatency());
    }

    static class TestMapStore extends MapStoreAdapter<Long, String> {

        public volatile CountDownLatch latch;

        @Override
        public void store(Long key, String value) {
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void storeAll(Map<Long, String> map) {
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public void delete(Long key) {
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
