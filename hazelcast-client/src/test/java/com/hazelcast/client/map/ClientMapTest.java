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
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapTest {

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    private static TestMapStore flushMapStore = new TestMapStore();
    private static TestMapStore transientMapStore = new TestMapStore();

    @BeforeClass
    public static void beforeClass() {
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
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testIssue537() throws InterruptedException {
        IMap<String, Object> map = createMap();

        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch nullLatch = new CountDownLatch(2);

        EntryListener<String, Object> listener = new EntryAdapter<String, Object>() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            public void entryEvicted(EntryEvent event) {
                final Object value = event.getValue();
                final Object oldValue = event.getOldValue();
                if (value != null) {
                    nullLatch.countDown();
                }
                if (oldValue != null) {
                    nullLatch.countDown();
                }
                latch.countDown();
            }
        };
        String id = map.addEntryListener(listener, true);

        map.put("key1", new GenericEvent("value1"), 2, TimeUnit.SECONDS);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(nullLatch.await(1, TimeUnit.SECONDS));

        map.removeEntryListener(id);

        map.put("key2", new GenericEvent("value2"));

        assertEquals(1, map.size());
    }

    @Test
    public void testContains() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertFalse(map.containsKey("key10"));
        assertTrue(map.containsKey("key1"));

        assertFalse(map.containsValue("value10"));
        assertTrue(map.containsValue("value1"));
    }

    @Test
    public void testGet() {
        IMap<String, String> map = createMap();
        fillMap(map);

        for (int i = 0; i < 10; i++) {
            assertEquals("value" + i, map.get("key" + i));
        }
    }

    @Test
    public void testRemoveAndDelete() {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertNull(map.remove("key10"));
        map.delete("key9");
        assertEquals(9, map.size());
        for (int i = 0; i < 9; i++) {
            assertEquals("value" + i, map.remove("key" + i));
        }
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame() {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertFalse(map.remove("key2", "value"));
        assertEquals(10, map.size());

        assertTrue(map.remove("key2", "value2"));
        assertEquals(9, map.size());
    }

    @Test
    public void testFlush() throws InterruptedException {
        flushMapStore.latch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap("flushMap");
        map.put(1l, "value");
        map.flush();

        assertOpenEventually(flushMapStore.latch, 5);
    }

    @Test
    public void testGetAllPutAll() {
        IMap<Integer, Integer> map = createMap();

        Map<Integer, Integer> filledMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100; i++) {
            filledMap.put(i, i);
        }
        map.putAll(filledMap);

        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals((int) map.get(i), i);
        }

        Set<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(3);
        Map tempMap = map.getAll(keySet);

        assertEquals(2, tempMap.size());
        assertEquals(1, tempMap.get(1));
        assertEquals(3, tempMap.get(3));
    }

    @Test
    public void testAsyncGet() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertEquals("value1", map.getAsync("key1").get());
    }

    @Test
    public void testAsyncPut() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertEquals("value3", map.putAsync("key3", "value").get());
        assertEquals("value", map.get("key3"));
    }

    @Test
    public void testAsyncPutWithTtl() throws Exception {
        IMap<String, String> map = createMap();

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<String> future = map.putAsync("key", "value1", 3, TimeUnit.SECONDS);
        String value = future.get();
        assertNull(value);
        assertEquals("value1", map.get("key"));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get("key"));
    }

    @Test
    public void testAsyncRemove() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);

        assertEquals("value4", map.removeAsync("key4").get());
        assertEquals(9, map.size());
    }

    @Test
    public void testTryPutRemove() throws Exception {
        final IMap<String, String> map = createMap();

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
        IMap<String, String> map = createMap();
        map.put("key1", "value1", 1, TimeUnit.SECONDS);

        assertNotNull(map.get("key1"));
        Thread.sleep(2000);
        assertNull(map.get("key1"));
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        IMap<String, String> map = createMap();

        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.putIfAbsent("key1", "value3"));
    }

    @Test
    public void testPutIfAbsentTtl() throws Exception {
        IMap<String, String> map = createMap();

        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        Thread.sleep(6000);
        assertNull(map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
    }

    @Test
    public void testSet() throws Exception {
        final IMap<String, String> map = createMap();
        map.set("key1", "value1");
        assertEquals("value1", map.get("key1"));

        map.set("key1", "value2");
        assertEquals("value2", map.get("key1"));

        map.set("key1", "value3", 1, TimeUnit.SECONDS);
        assertEquals("value3", map.get("key1"));

        Thread.sleep(2000);
        assertNull(map.get("key1"));
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
        final IMap<String, String> map = createMap();
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
        final IMap<String, String> map = createMap();
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
        final IMap<String, String> map = createMap();
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
        final IMap<String, String> map = createMap();

        assertTrue(map.tryLock("key1", 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (!map.tryLock("key1", 2, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertTrue(map.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (map.tryLock("key1", 20, TimeUnit.SECONDS)) {
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        Thread.sleep(1000);
        map.unlock("key1");
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(map.isLocked("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        final IMap<String, String> map = createMap();
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
        IMap<String, String> map = createMap();
        fillMap(map);

        Collection values = map.values(new SqlPredicate("this == value1"));
        assertEquals(1, values.size());
        assertEquals("value1", values.iterator().next());
    }

    @Test
    public void testReplace() throws Exception {
        IMap<String, String> map = createMap();

        assertNull(map.replace("key1", "value1"));
        map.put("key1", "value1");
        assertEquals("value1", map.replace("key1", "value2"));
        assertEquals("value2", map.get("key1"));

        assertFalse(map.replace("key1", "value1", "value3"));
        assertEquals("value2", map.get("key1"));
        assertTrue(map.replace("key1", "value2", "value3"));
        assertEquals("value3", map.get("key1"));
    }

    @Test
    public void testSubmitToKey() throws Exception {
        IMap<Integer, Object> map = createMap();
        map.put(1, 1);
        Future future = map.submitToKey(1, new IncrementEntryProcessor());

        assertEquals(2, future.get());
        assertEquals(2, map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        IMap<Integer, Object> map = createMap();
        Future future = map.submitToKey(11, new IncrementEntryProcessor());

        assertEquals(1, future.get());
        assertEquals(1, map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        IMap<Integer, Object> map = createMap();
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

        map.submitToKey(1, new IncrementEntryProcessor(), executionCallback);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, map.get(1));
    }

    @Test
    public void testListener() throws InterruptedException {
        IMap<String, String> map = createMap();

        final CountDownLatch latch1Add = new CountDownLatch(5);
        final CountDownLatch latch1Remove = new CountDownLatch(2);

        final CountDownLatch latch2Add = new CountDownLatch(1);
        final CountDownLatch latch2Remove = new CountDownLatch(1);

        EntryListener<String, String> listener1 = new EntryAdapter<String, String>() {
            public void entryAdded(EntryEvent event) {
                latch1Add.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                latch1Remove.countDown();
            }
        };

        EntryListener<String, String> listener2 = new EntryAdapter<String, String>() {
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
        IMap<AuthenticationRequest, Object> tradeMap = createMap();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        EntryListener<AuthenticationRequest, Object> listener = new EntryAdapter<AuthenticationRequest, Object>() {
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
        IMap<String, String> map = createMap();
        fillMap(map);

        Collection<String> collection = map.values(new SqlPredicate("this == value1"));
        assertEquals("value1", collection.iterator().next());

        Set<String> set = map.keySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set.iterator().next());

        Set<Map.Entry<String, String>> set1 = map.entrySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set1.iterator().next().getKey());
        assertEquals("value1", set1.iterator().next().getValue());
    }

    /**
     * Issue #923
     */
    @Test
    public void testPartitionAwareKey() {
        String name = randomString();
        PartitionAwareKey key = new PartitionAwareKey("123");
        String value = "value";

        IMap<Object, Object> map1 = server.getMap(name);
        map1.put(key, value);
        assertEquals(value, map1.get(key));

        IMap<Object, Object> map2 = client.getMap(name);
        assertEquals(value, map2.get(key));
    }

    private static class PartitionAwareKey implements PartitionAware, Serializable {
        private final String pk;

        private PartitionAwareKey(String pk) {
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

        IMap<Integer, Integer> map1 = client.getMap(name);
        IMap<Integer, Integer> map2 = client.getMap(name);

        for (int i = 0; i < 10; i++) {
            map1.put(i, 0);
        }

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);

        Map<Integer, Object> resultMap = map2.executeOnKeys(keys, new IncrementEntryProcessor());
        assertEquals(1, resultMap.get(1));
        assertEquals(1, resultMap.get(4));
        assertEquals(1, resultMap.get(7));
        assertEquals(1, resultMap.get(9));
        assertEquals(1, (int) map1.get(1));
        assertEquals(0, (int) map1.get(2));
        assertEquals(0, (int) map1.get(3));
        assertEquals(1, (int) map1.get(4));
        assertEquals(0, (int) map1.get(5));
        assertEquals(0, (int) map1.get(6));
        assertEquals(1, (int) map1.get(7));
        assertEquals(0, (int) map1.get(8));
        assertEquals(1, (int) map1.get(9));
    }

    @Test
    public void testListeners_clearAllFromNode() {
        String name = randomString();

        MultiMap<Integer, Deal> multiMap = client.getMultiMap(name);
        CountDownLatch gateClearAll = new CountDownLatch(1);
        CountDownLatch gateAdd = new CountDownLatch(1);
        EntryListener<Integer, Deal> listener = new EntListener(gateAdd, null, null, null, gateClearAll, null);

        multiMap.addEntryListener(listener, false);
        multiMap.put(1, new Deal(2));
        server.getMultiMap(name).clear();

        assertOpenEventually(gateAdd);
        assertOpenEventually(gateClearAll);
    }

    /**
     * Issue #996
     */
    @Test
    public void testEntryListener() throws InterruptedException {
        CountDownLatch gateAdd = new CountDownLatch(3);
        CountDownLatch gateRemove = new CountDownLatch(1);
        CountDownLatch gateEvict = new CountDownLatch(1);
        CountDownLatch gateUpdate = new CountDownLatch(1);
        CountDownLatch gateClearAll = new CountDownLatch(1);
        CountDownLatch gateEvictAll = new CountDownLatch(1);

        String mapName = randomString();

        IMap<Integer, Deal> serverMap = server.getMap(mapName);
        serverMap.put(3, new Deal(3));

        IMap<Integer, Deal> clientMap = client.getMap(mapName);

        assertEquals(1, clientMap.size());

        EntryListener<Integer, Deal> listener = new EntListener(gateAdd, gateRemove, gateEvict, gateUpdate, gateClearAll, gateEvictAll);

        clientMap.addEntryListener(listener, new SqlPredicate("id=1"), 2, true);
        clientMap.put(2, new Deal(1));
        clientMap.put(2, new Deal(1));
        clientMap.remove(2);

        clientMap.put(2, new Deal(1));
        clientMap.evict(2);

        clientMap.clear();
        clientMap.put(2, new Deal(1));
        clientMap.evictAll();

        assertTrue(gateAdd.await(10, TimeUnit.SECONDS));
        assertTrue(gateRemove.await(10, TimeUnit.SECONDS));
        assertTrue(gateEvict.await(10, TimeUnit.SECONDS));
        assertTrue(gateUpdate.await(10, TimeUnit.SECONDS));
        assertTrue(gateClearAll.await(10, TimeUnit.SECONDS));
        assertTrue(gateEvictAll.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMapStatistics() throws Exception {
        String name = randomString();
        LocalMapStats localMapStats = server.getMap(name).getLocalMapStats();
        IMap<Integer, Integer> map = client.getMap(name);

        int operationCount = 1000;
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

    @Test
    public void testEntryListenerWithPredicateOnDeleteOperation() throws Exception {
        final IMap<Object, Object> serverMap = server.getMap("A");
        final IMap<Object, Object> clientMap = client.getMap("A");
        final CountDownLatch latch = new CountDownLatch(1);
        clientMap.addEntryListener(new EntryAdapter<Object, Object>() {
            public void entryRemoved(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, new TestPredicate(), true);
        serverMap.put("A", "B");
        clientMap.delete("A");
        assertOpenEventually(latch, 10);
    }

    private <K, V> IMap<K, V> createMap() {
        return client.getMap(randomString());
    }

    private void fillMap(IMap<String, String> map) {
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, "value" + i);
        }
    }

    private static final class TestPredicate implements Predicate<Object, Object>, Serializable {

        @Override
        public boolean apply(Map.Entry<Object, Object> mapEntry) {
            assert mapEntry != null;
            return mapEntry.getKey().equals("A");
        }
    }

    private static class TestMapStore extends MapStoreAdapter<Long, String> {

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

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Object, Integer> implements DataSerializable {
        IncrementEntryProcessor() {
            super(true);
        }

        @Override
        public Object process(Map.Entry<Object, Integer> entry) {
            Integer value = entry.getValue();
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
    }

    private static class EntListener implements EntryListener<Integer, Deal>, Serializable {
        private final CountDownLatch _gateAdd;
        private final CountDownLatch _gateRemove;
        private final CountDownLatch _gateEvict;
        private final CountDownLatch _gateUpdate;
        private final CountDownLatch _gateClearAll;
        private final CountDownLatch _gateEvictAll;

        EntListener(CountDownLatch gateAdd, CountDownLatch gateRemove, CountDownLatch gateEvict,
                    CountDownLatch gateUpdate, CountDownLatch gateClearAll, CountDownLatch gateEvictAll) {
            _gateAdd = gateAdd;
            _gateRemove = gateRemove;
            _gateEvict = gateEvict;
            _gateUpdate = gateUpdate;
            _gateClearAll = gateClearAll;
            _gateEvictAll = gateEvictAll;
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
        public void mapEvicted(MapEvent event) {
            _gateEvictAll.countDown();
        }

        @Override
        public void mapCleared(MapEvent event) {
            _gateClearAll.countDown();
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

    private static class Deal implements Serializable {
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

    private static class GenericEvent implements Serializable {

        private final String value;

        public GenericEvent(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
