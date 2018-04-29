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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.map.helpers.GenericEvent;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.NamedPortable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.TestSerializationConstants;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private TestMapStore flushMapStore = new TestMapStore();
    private TestMapStore transientMapStore = new TestMapStore();

    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = getConfig();
        config.getMapConfig("flushMap").
                setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1000)
                        .setImplementation(flushMapStore));
        config.getMapConfig("putTransientMap").
                setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1000)
                        .setImplementation(transientMapStore));

        server = hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = getClientConfig();
        clientConfig.getSerializationConfig()
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new NamedPortable();
                    }

                });
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testIssue537() {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch nullLatch = new CountDownLatch(2);

        EntryListener<String, GenericEvent> listener = new EntryAdapter<String, GenericEvent>() {
            @Override
            public void entryAdded(EntryEvent<String, GenericEvent> event) {
                latch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<String, GenericEvent> event) {
                GenericEvent value = event.getValue();
                GenericEvent oldValue = event.getOldValue();
                if (value == null) {
                    nullLatch.countDown();
                }
                if (oldValue != null) {
                    nullLatch.countDown();
                }
                latch.countDown();
            }
        };

        IMap<String, GenericEvent> map = createMap();
        String id = map.addEntryListener(listener, true);

        map.put("key1", new GenericEvent("value1"), 2, TimeUnit.SECONDS);

        assertOpenEventually(latch);
        assertOpenEventually(nullLatch);

        map.removeEntryListener(id);
        map.put("key2", new GenericEvent("value2"));
        assertEquals(1, map.size());
    }

    @Test
    public void testSerializationServiceNullClassLoaderProblem() {
        IMap<Integer, SampleTestObjects.PortableEmployee> map = client.getMap("test");

        // If the classloader is null the following call throws NullPointerException
        map.values(new InstanceOfPredicate(SampleTestObjects.PortableEmployee.class));
    }

    @Test
    public void testContains() {
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
            String actual = map.get("key" + i);
            assertEquals("value" + i, actual);
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
            String actual = map.remove("key" + i);
            assertEquals("value" + i, actual);
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
    public void testFlush() {
        flushMapStore.latch = new CountDownLatch(1);
        IMap<Long, String> map = client.getMap("flushMap");
        map.put(1L, "value");
        map.flush();
        assertOpenEventually(flushMapStore.latch);
    }

    @Test
    public void testGetAllPutAll() {
        Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100; i++) {
            expectedMap.put(i, i);
        }

        IMap<Integer, Integer> map = createMap();
        map.putAll(expectedMap);
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            int actual = map.get(i);
            assertEquals(i, actual);
        }

        Set<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(3);
        Map getAllMap = map.getAll(keySet);
        assertEquals(2, getAllMap.size());
        assertEquals(1, getAllMap.get(1));
        assertEquals(3, getAllMap.get(3));
    }

    @Test
    public void testPutAllWithTooManyEntries() {
        Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 1000; i++) {
            expectedMap.put(i, i);
        }

        IMap<Integer, Integer> map = createMap();
        map.putAll(expectedMap);
        assertEquals(map.size(), 1000);
        for (int i = 0; i < 1000; i++) {
            int actual = map.get(i);
            assertEquals(i, actual);
        }
    }

    @Test
    public void testAsyncGet() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);

        Future<String> future = map.getAsync("key1");
        assertEquals("value1", future.get());
    }

    @Test
    public void testAsyncPut() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);
        Future<String> future = map.putAsync("key3", "value");
        assertEquals("value3", future.get());
        assertEquals("value", map.get("key3"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAsyncPutWithTtl() throws Exception {
        IMap<String, String> map = createMap();
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<String> future = map.putAsync("key", "value1", 3, TimeUnit.SECONDS);
        assertNull(future.get());
        assertEquals("value1", map.get("key"));

        assertOpenEventually(latch);
        assertNull(map.get("key"));
    }

    @Test
    public void testAsyncSet() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);
        Future<Void> future = map.setAsync("key3", "value");
        future.get();
        assertEquals("value", map.get("key3"));
    }

    @Test
    public void testAsyncSetWithTtl() throws Exception {
        IMap<String, String> map = createMap();
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryEvictedListener<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<Void> future = map.setAsync("key", "value1", 3, TimeUnit.SECONDS);
        future.get();
        assertEquals("value1", map.get("key"));

        assertOpenEventually(latch);
        assertNull(map.get("key"));
    }

    @Test
    public void testAsyncRemove() throws Exception {
        IMap<String, String> map = createMap();
        fillMap(map);
        Future<String> future = map.removeAsync("key4");
        assertEquals("value4", future.get());
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
            @Override
            public void run() {
                boolean result = map.tryPut("key1", "value3", 1, TimeUnit.SECONDS);
                if (!result) {
                    latch.countDown();
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                boolean result = map.tryRemove("key2", 1, TimeUnit.SECONDS);
                if (!result) {
                    latch.countDown();
                }
            }
        }.start();

        assertOpenEventually(latch);
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        map.forceUnlock("key1");
        map.forceUnlock("key2");

    }

    @Test
    public void testPutTtl() {
        final IMap<String, String> map = createMap();
        map.put("key1", "value1", 1, TimeUnit.SECONDS);
        assertNotNull(map.get("key1"));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get("key1"));
            }
        });
    }

    @Test
    public void testPutIfAbsent() {
        IMap<String, String> map = createMap();
        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.putIfAbsent("key1", "value3"));
    }

    @Test
    public void testPutIfAbsentTtl() {
        final IMap<String, String> map = createMap();
        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get("key1"));
            }
        });
        assertNull(map.putIfAbsent("key1", "value3", 10, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
    }

    @Test
    public void testSet() {
        final IMap<String, String> map = createMap();
        map.set("key1", "value1");
        assertEquals("value1", map.get("key1"));

        map.set("key1", "value2");
        assertEquals("value2", map.get("key1"));

        map.set("key1", "value3", 1, TimeUnit.SECONDS);
        assertEquals("value3", map.get("key1"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get("key1"));
            }
        });
    }

    @Test
    public void testPutTransient() throws Exception {
        transientMapStore.latch = new CountDownLatch(1);
        IMap<Long, String> map = client.getMap("putTransientMap");
        map.putTransient(3L, "value1", 100, TimeUnit.SECONDS);
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
            @Override
            public void run() {
                map.tryPut("key1", "value2", 1, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertOpenEventually(latch);
        assertEquals("value1", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testTryLock() throws Exception {
        final IMap<String, String> map = createMap();
        assertTrue(map.tryLock("key1", 2, TimeUnit.SECONDS));

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
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
        assertOpenEventually(latch);
        assertTrue(map.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread() {
            @Override
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
        assertOpenEventually(latch2);
        assertTrue(map.isLocked("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        final IMap<String, String> map = createMap();
        map.lock("key1");

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                map.forceUnlock("key1");
                latch.countDown();
            }
        }.start();
        assertOpenEventually(latch);
        assertFalse(map.isLocked("key1"));
    }

    @Test
    public void testValuesWithPredicate() {
        IMap<String, String> map = createMap();
        fillMap(map);

        Collection values = map.values(new SqlPredicate("this == value1"));
        assertEquals(1, values.size());
        assertEquals("value1", values.iterator().next());
    }

    @Test
    public void testValues() {
        IMap<String, String> map = createMap();
        fillMap(map);

        Collection values = map.values();
        assertEquals(10, values.size());
    }

    @Test
    public void testReplace() {
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
    public void testExecuteOnKey() {
        IMap<Integer, Integer> map = createMap();
        map.put(1, 1);
        assertEquals(2, map.executeOnKey(1, new IncrementerEntryProcessor()));
    }

    @Test
    public void testSubmitToKey() throws Exception {
        IMap<Integer, Integer> map = createMap();
        map.put(1, 1);
        Future future = map.submitToKey(1, new IncrementerEntryProcessor());
        assertEquals(2, future.get());

        int actual = map.get(1);
        assertEquals(2, actual);
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        IMap<Integer, Integer> map = createMap();
        Future future = map.submitToKey(11, new IncrementerEntryProcessor());
        assertEquals(1, future.get());

        int actual = map.get(11);
        assertEquals(1, actual);
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        IMap<Integer, Integer> map = createMap();
        map.put(1, 1);

        final AtomicInteger result = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutionCallback<Integer> executionCallback = new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                result.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        map.submitToKey(1, new IncrementerEntryProcessor(), executionCallback);
        assertOpenEventually(latch);
        assertEquals(2, result.get());

        int actual = map.get(1);
        assertEquals(2, actual);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testListener() throws Exception {
        IMap<String, String> map = createMap();
        final CountDownLatch latch1Add = new CountDownLatch(5);
        final CountDownLatch latch1Remove = new CountDownLatch(2);
        EntryListener<String, String> listener1 = new EntryAdapter<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                latch1Add.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<String, String> event) {
                latch1Remove.countDown();
            }
        };

        final CountDownLatch latch2Add = new CountDownLatch(1);
        final CountDownLatch latch2Remove = new CountDownLatch(1);
        EntryListener<String, String> listener2 = new EntryAdapter<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                latch2Add.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<String, String> event) {
                latch2Remove.countDown();
            }
        };

        map.addEntryListener(listener1, false);
        map.addEntryListener(listener2, "key3", true);

        sleepSeconds(1);

        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");
        map.put("key5", "value5");

        map.remove("key1");
        map.remove("key3");

        assertOpenEventually(latch1Add);
        assertOpenEventually(latch1Remove);
        assertOpenEventually(latch2Add);
        assertOpenEventually(latch2Remove);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPredicateListenerWithPortableKey() throws Exception {
        IMap<Portable, Integer> tradeMap = createMap();

        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        EntryListener listener = new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                atomicInteger.incrementAndGet();
                countDownLatch.countDown();
            }
        };

        NamedPortable key = new NamedPortable("a", 1);
        tradeMap.addEntryListener(listener, key, true);
        NamedPortable key2 = new NamedPortable("b", 2);
        tradeMap.put(key2, 1);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(0, atomicInteger.get());
    }

    @Test
    public void testBasicPredicate() {
        IMap<String, String> map = createMap();
        fillMap(map);

        Collection collection = map.values(new SqlPredicate("this == value1"));
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

        IMap<PartitionAwareKey, String> map1 = server.getMap(name);
        map1.put(key, value);
        assertEquals(value, map1.get(key));

        IMap<PartitionAwareKey, String> map2 = client.getMap(name);
        assertEquals(value, map2.get(key));
    }

    @Test
    public void testExecuteOnKeys() {
        String name = randomString();
        IMap<Integer, Integer> map = client.getMap(name);
        IMap<Integer, Integer> map2 = client.getMap(name);

        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }
        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);

        Map<Integer, Object> resultMap = map2.executeOnKeys(keys, new IncrementerEntryProcessor());
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

    @Test
    public void testListeners_clearAllFromNode() {
        CountDownLatch gateAdd = new CountDownLatch(1);
        CountDownLatch gateClearAll = new CountDownLatch(1);
        EntryListener<Integer, Deal> listener = new IntegerDealEntryListener(gateAdd, null, null, null, gateClearAll, null);

        String name = randomString();
        MultiMap<Integer, Deal> multiMap = client.getMultiMap(name);
        multiMap.addEntryListener(listener, false);
        multiMap.put(1, new Deal(1));

        server.getMultiMap(name).clear();
        assertOpenEventually(gateAdd);
        assertOpenEventually(gateClearAll);
    }

    /**
     * Issue #996
     */
    @Test
    @SuppressWarnings({"deprecation", "unchecked"})
    public void testEntryListener() throws Exception {
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

        EntryListener listener = new IntegerDealEntryListener(gateAdd, gateRemove, gateEvict, gateUpdate, gateClearAll,
                gateEvictAll);

        clientMap.addEntryListener(listener, new SqlPredicate("id=1"), 2, true);
        clientMap.put(2, new Deal(1));
        clientMap.put(2, new Deal(1));
        clientMap.remove(2);

        clientMap.put(2, new Deal(1));
        clientMap.evict(2);

        clientMap.clear();
        clientMap.put(2, new Deal(1));
        clientMap.evictAll();

        assertOpenEventually(gateAdd);
        assertOpenEventually(gateRemove);
        assertOpenEventually(gateEvict);
        assertOpenEventually(gateUpdate);
        assertOpenEventually(gateClearAll);
        assertOpenEventually(gateEvictAll);
    }

    @Test
    public void testMapStatistics() {
        String name = randomString();
        IMap<Integer, Integer> map = client.getMap(name);

        int operationCount = 1000;
        for (int i = 0; i < operationCount; i++) {
            map.put(i, i);
            map.get(i);
            map.remove(i);
        }

        LocalMapStats localMapStats = server.getMap(name).getLocalMapStats();
        assertEquals("put count", operationCount, localMapStats.getPutOperationCount());
        assertEquals("get count", operationCount, localMapStats.getGetOperationCount());
        assertEquals("remove count", operationCount, localMapStats.getRemoveOperationCount());
        assertTrue("put latency", 0 < localMapStats.getTotalPutLatency());
        assertTrue("get latency", 0 < localMapStats.getTotalGetLatency());
        assertTrue("remove latency", 0 < localMapStats.getTotalRemoveLatency());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testEntryListenerWithPredicateOnDeleteOperation() {
        IMap<String, String> serverMap = server.getMap("A");
        IMap<String, String> clientMap = client.getMap("A");

        final CountDownLatch latch = new CountDownLatch(1);
        clientMap.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void entryRemoved(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, new TestPredicate(), true);

        serverMap.put("A", "B");
        clientMap.delete("A");
        assertOpenEventually(latch);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> IMap<K, V> createMap() {
        return client.getMap(randomString());
    }

    private void fillMap(IMap<String, String> map) {
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, "value" + i);
        }
    }

    private static class IncrementerEntryProcessor extends AbstractEntryProcessor<Integer, Integer> implements DataSerializable {

        IncrementerEntryProcessor() {
            super(true);
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
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

    private static class PartitionAwareKey implements PartitionAware<String>, Serializable {

        private final String partitionKey;

        private PartitionAwareKey(String partitionKey) {
            this.partitionKey = partitionKey;
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }
    }

    private static class IntegerDealEntryListener implements EntryListener<Integer, Deal>, Serializable {

        private final CountDownLatch _gateAdd;
        private final CountDownLatch _gateRemove;
        private final CountDownLatch _gateEvict;
        private final CountDownLatch _gateUpdate;
        private final CountDownLatch _gateClearAll;
        private final CountDownLatch _gateEvictAll;

        IntegerDealEntryListener(CountDownLatch gateAdd, CountDownLatch gateRemove, CountDownLatch gateEvict,
                                 CountDownLatch gateUpdate, CountDownLatch gateClearAll, CountDownLatch gateEvictAll) {
            _gateAdd = gateAdd;
            _gateRemove = gateRemove;
            _gateEvict = gateEvict;
            _gateUpdate = gateUpdate;
            _gateClearAll = gateClearAll;
            _gateEvictAll = gateEvictAll;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Deal> event) {
            _gateAdd.countDown();
        }

        @Override
        public void entryEvicted(EntryEvent<Integer, Deal> event) {
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
        public void entryRemoved(EntryEvent<Integer, Deal> event) {
            _gateRemove.countDown();
        }

        @Override
        public void entryUpdated(EntryEvent<Integer, Deal> event) {
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

    private static class TestPredicate implements Predicate<String, String>, Serializable {

        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
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
}
