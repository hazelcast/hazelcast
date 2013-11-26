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
import com.hazelcast.core.*;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author ali 5/22/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapTest {

    static final String name = "test";
    static HazelcastInstance client;
    static HazelcastInstance server;
    static IMap map;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient(null);
        map = client.getMap(name);
    }

    @AfterClass
    public static void destroy() {
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws Exception {
        map.clear();
    }

    @Test
    public void testIssue537() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch nullLatch = new CountDownLatch(2);

        final EntryListener listener = new EntryListener() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            public void entryRemoved(EntryEvent event) {
            }

            public void entryUpdated(EntryEvent event) {
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
        final String id = map.addEntryListener(listener, true);

        map.put("key1", new GenericEvent("value1"), 2, TimeUnit.SECONDS);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(nullLatch.await(1, TimeUnit.SECONDS));

        map.removeEntryListener(id);

        map.put("key2", new GenericEvent("value2"));

        assertEquals(1, map.size());

    }

    @Test
    public void testContains() throws Exception {

        fillMap();

        assertFalse(map.containsKey("key10"));
        assertTrue(map.containsKey("key1"));

        assertFalse(map.containsValue("value10"));
        assertTrue(map.containsValue("value1"));

    }

    @Test
    public void testGet() {
        fillMap();
        for (int i = 0; i < 10; i++) {
            Object o = map.get("key" + i);
            assertEquals("value" + i, o);
        }
    }

    @Test
    public void testRemoveAndDelete() {
        fillMap();
        assertNull(map.remove("key10"));
        map.delete("key9");
        assertEquals(9, map.size());
        for (int i = 0; i < 9; i++) {
            Object o = map.remove("key" + i);
            assertEquals("value" + i, o);
        }
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame() {
        fillMap();
        assertFalse(map.remove("key2", "value"));
        assertEquals(10, map.size());

        assertTrue(map.remove("key2", "value2"));
        assertEquals(9, map.size());
    }

    @Test
    public void flush() {
        //TODO map store
    }

    @Test
    public void testGetAllPutAll() {
        Map mm = new HashMap();
        for (int i = 0; i < 100; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i);
        }

        Set ss = new HashSet();
        ss.add(1);
        ss.add(3);
        Map m2 = map.getAll(ss);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
    }

    @Test
    public void testAsyncGet() throws Exception {
        fillMap();
        Future f = map.getAsync("key1");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
        }
        assertNull(o);
        o = f.get();
        assertEquals("value1", o);
    }

    @Test
    public void testAsyncPut() throws Exception {
        fillMap();
        Future f = map.putAsync("key3", "value");

        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
        }
        assertNull(o);
        o = f.get();
        assertEquals("value3", o);
        assertEquals("value", map.get("key3"));
    }


    @Test
    public void testAsyncPutWithTtl() throws Exception {
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
        fillMap();
        Future f = map.removeAsync("key4");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
        }
        assertNull(o);
        o = f.get();
        assertEquals("value4", o);
        assertEquals(9, map.size());
    }

    @Test
    public void testTryPutRemove() throws Exception {
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
        map.put("key1", "value1", 1, TimeUnit.SECONDS);
        assertNotNull(map.get("key1"));
        Thread.sleep(2000);
        assertNull(map.get("key1"));
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.putIfAbsent("key1", "value3"));
    }

    @Test
    public void testPutIfAbsentTtl() throws Exception {
        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
        assertNull(map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
    }

    @Test
    public void testSet() throws Exception {
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
    public void testPutTransient() throws Exception {
        //TODO mapstore
    }

    @Test
    public void testLock() throws Exception {
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
//        final IMap tempMap = server.getMap(name);
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
        fillMap();

        final Collection values = map.values(new SqlPredicate("this == value1"));
        assertEquals(1, values.size());
        assertEquals("value1", values.iterator().next());
    }

    @Test
    public void testReplace() throws Exception {
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
        map.put(1,1);
        Future f = map.submitToKey(1, new IncrementorEntryProcessor());
        assertEquals(2,f.get());
        assertEquals(2,map.get(1));
    }
    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        Future f = map.submitToKey(11, new IncrementorEntryProcessor());
        assertEquals(1,f.get());
        assertEquals(1,map.get(11));
    }
    @Test
    public void testSubmitToKeyWithCallback() throws  Exception
    {
        map.put(1,1);
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

        map.submitToKey(1,new IncrementorEntryProcessor(),executionCallback);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2,map.get(1));
    }

    @Test
    public void testListener() throws InterruptedException {
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
        IMap tradeMap = client.getMap("tradeMap");
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        EntryListener listener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                atomicInteger.incrementAndGet();
                countDownLatch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        };
        final AuthenticationRequest key = new AuthenticationRequest(new UsernamePasswordCredentials("a", "b"));
        tradeMap.addEntryListener(listener, key, true);
        final AuthenticationRequest key2 = new AuthenticationRequest(new UsernamePasswordCredentials("a", "c"));
        tradeMap.put(key2, 1);
        assertFalse(countDownLatch.await(15, TimeUnit.SECONDS));
        assertEquals(0,atomicInteger.get());
    }

    @Test
    public void testBasicPredicate() {
        fillMap();
        final Collection collection = map.values(new SqlPredicate("this == value1"));
        assertEquals("value1", collection.iterator().next());
        final Set set = map.keySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set.iterator().next());
        final Set<Map.Entry<String, String>> set1 = map.entrySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set1.iterator().next().getKey());
        assertEquals("value1", set1.iterator().next().getValue());
    }

    private void fillMap() {
        for (int i = 0; i < 10; i++) {
            map.put("key" + i, "value" + i);
        }
    }

    /**
     * Issue #923
     */
    @Test
    public void testPartitionAwareKey() {
        String name = "testPartitionAwareKey";
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


    /**
     * Issue #996
     */
    @Test
    public void testEntryListener() throws InterruptedException {
        final CountDownLatch gateAdd = new CountDownLatch(2);
        final CountDownLatch gateRemove = new CountDownLatch(1);
        final CountDownLatch gateEvict = new CountDownLatch(1);
        final CountDownLatch gateUpdate = new CountDownLatch(1);

        final String mapName = "testEntryListener";

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


}
