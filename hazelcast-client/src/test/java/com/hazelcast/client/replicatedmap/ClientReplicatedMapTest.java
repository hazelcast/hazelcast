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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
//TODO
public class ClientReplicatedMapTest extends HazelcastTestSupport {

    private static final Comparator<Entry<Integer, Integer>> ENTRYSET_COMPARATOR = new Comparator<Entry<Integer, Integer>>() {
        @Override
        public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    @After
    public void shutdown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAddObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testNearCacheObjectObject() throws Exception {

        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);

        ClientConfig ccfg = new ClientConfig();
        ccfg.addNearCacheConfig("default", new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT).setInvalidateOnChange(true));
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient(ccfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        String value2 = map2.get("foo");
        assertEquals("bar", value);

        assertSame(value, value2);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("bar");
        assertEquals("foo", value);

        value2 = map2.get("bar");
        assertEquals("foo", value);

        assertSame(value, value2);
    }

    @Test
    public void testNearCacheObjectBinary() throws Exception {

        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);

        ClientConfig ccfg = new ClientConfig();
        ccfg.addNearCacheConfig("default", new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.BINARY).setInvalidateOnChange(true));
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient(ccfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        String value2 = map2.get("foo");
        assertEquals("bar", value);

        assertNotSame(value, value2);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("bar");
        assertEquals("foo", value);

        value2 = map2.get("bar");
        assertEquals("foo", value);

        assertNotSame(value, value2);
    }

    @Test
    public void testAddTtlObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo"));
        TimeUnit.SECONDS.sleep(5);

        assertNull(map1.get("foo"));
        assertNull(map2.get("foo"));
    }

    @Test
    public void testUpdateObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testUpdateTtlObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo2", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo2"));

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar2");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo2", "bar2", 1, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(5);

        assertEquals("bar2", map1.get("foo"));
        assertEquals("bar2", map2.get("foo"));

        assertNull(map1.get("foo2"));
        assertNull(map2.get("foo2"));
    }

    @Test
    public void testRemoveObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 2, EntryEventType.REMOVED, map1, map2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testEntryListenerObject() throws Exception {
        final CountDownLatch added = new CountDownLatch(2);
        final CountDownLatch updated = new CountDownLatch(2);
        final CountDownLatch removed = new CountDownLatch(2);

        EntryListener listener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                added.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                removed.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                updated.countDown();;
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        };

        
        Config cfg = new Config();
        ListenerConfig listenerConfig = new ListenerConfig().setImplementation(listener);
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT)
                .getListenerConfigs().add(listenerConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");
        map2.addEntryListener(listener);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 2, EntryEventType.REMOVED, map1, map2);

        added.await();
        updated.await();
        removed.await();
    }

    @Test
    public void testSizeObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertEquals(testValues.length, map1.size());
        assertEquals(testValues.length, map2.size());
    }

    @Test
    public void testContainsKeyObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsKey(testValues[0].getKey()));
        assertTrue(map2.containsKey(testValues[0].getKey()));
    }

    @Test
    public void testContainsValueObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsValue(testValues[0].getValue()));
        assertTrue(map2.containsValue(testValues[0].getValue()));
    }

    @Test
    public void testValuesObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> values1 = new ArrayList<Integer>(map1.values());
        List<Integer> values2 = new ArrayList<Integer>(map2.values());

        Collections.sort(values1);
        Collections.sort(values2);
        Collections.sort(valuesTestValues);

        assertEquals(valuesTestValues, values1);
        assertEquals(valuesTestValues, values2);
    }

    @Test
    public void testKeySetObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    keySetTestValues.add(entry.getKey());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> keySet1 = new ArrayList<Integer>(map1.keySet());
        List<Integer> keySet2 = new ArrayList<Integer>(map2.keySet());

        Collections.sort(keySet1);
        Collections.sort(keySet2);
        Collections.sort(keySetTestValues);

        assertEquals(keySetTestValues, keySet1);
        assertEquals(keySetTestValues, keySet2);
    }

    @Test
    public void testEntrySetObject() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    @Test
    public void testAddBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testNearCacheBinaryObject() throws Exception {

        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);

        ClientConfig ccfg = new ClientConfig();
        ccfg.addNearCacheConfig("default", new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT).setInvalidateOnChange(true));
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient(ccfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        String value2 = map2.get("foo");
        assertEquals("bar", value);

        assertSame(value, value2);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("bar");
        assertEquals("foo", value);

        value2 = map2.get("bar");
        assertEquals("foo", value);

        assertSame(value, value2);
    }

    @Test
    public void testNearCacheBinaryBinary() throws Exception {

        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);

        ClientConfig ccfg = new ClientConfig();
        ccfg.addNearCacheConfig("default", new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.BINARY).setInvalidateOnChange(true));
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient(ccfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        String value2 = map2.get("foo");
        assertEquals("bar", value);

        assertNotSame(value, value2);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("bar");
        assertEquals("foo", value);

        value2 = map2.get("bar");
        assertEquals("foo", value);

        assertNotSame(value, value2);
    }

    @Test
    public void testAddTtlBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo"));
        TimeUnit.SECONDS.sleep(5);

        assertNull(map1.get("foo"));
        assertNull(map2.get("foo"));
    }

    @Test
    public void testUpdateBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testUpdateTtlBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo2", "bar", 3, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo2"));

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar2");
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo2", "bar2", 1, TimeUnit.SECONDS);
            }
        }, 2, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(5);

        assertEquals("bar2", map1.get("foo"));
        assertEquals("bar2", map2.get("foo"));

        assertNull(map1.get("foo2"));
        assertNull(map2.get("foo2"));
    }

    @Test
    public void testRemoveBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 2, EntryEventType.REMOVED, map1, map2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testEntryListenerBinary() throws Exception {
        final CountDownLatch added = new CountDownLatch(2);
        final CountDownLatch updated = new CountDownLatch(2);
        final CountDownLatch removed = new CountDownLatch(2);

        EntryListener listener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                added.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                removed.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                updated.countDown();;
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        };

        
        Config cfg = new Config();
        ListenerConfig listenerConfig = new ListenerConfig().setImplementation(listener);
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY)
                .getListenerConfigs().add(listenerConfig);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");
        map2.addEntryListener(listener);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 2, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 2, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 2, EntryEventType.REMOVED, map1, map2);

        added.await();
        updated.await();
        removed.await();
    }

    @Test
    public void testSizeBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertEquals(testValues.length, map1.size());
        assertEquals(testValues.length, map2.size());
    }

    @Test
    public void testContainsKeyBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsKey(testValues[0].getKey()));
        assertTrue(map2.containsKey(testValues[0].getKey()));
    }

    @Test
    public void testContainsValueBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsValue(testValues[0].getValue()));
        assertTrue(map2.containsValue(testValues[0].getValue()));
    }

    @Test
    public void testValuesBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> values1 = new ArrayList<Integer>(map1.values());
        List<Integer> values2 = new ArrayList<Integer>(map2.values());

        Collections.sort(values1);
        Collections.sort(values2);
        Collections.sort(valuesTestValues);

        assertEquals(valuesTestValues, values1);
        assertEquals(valuesTestValues, values2);
    }

    @Test
    public void testKeySetBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    keySetTestValues.add(entry.getKey());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> keySet1 = new ArrayList<Integer>(map1.keySet());
        List<Integer> keySet2 = new ArrayList<Integer>(map2.keySet());

        Collections.sort(keySet1);
        Collections.sort(keySet2);
        Collections.sort(keySetTestValues);

        assertEquals(keySetTestValues, keySet1);
        assertEquals(keySetTestValues, keySet2);
    }

    @Test
    public void testEntrySetBinary() throws Exception {
        
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = HazelcastClient.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    private SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random();
        SimpleEntry<Integer, Integer>[] testValues = new SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }

}
