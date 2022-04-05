/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.mapstore.MapStoreTest.TestMapStore;
import com.hazelcast.map.impl.mapstore.writebehind.MapStoreWriteBehindTest.FailAwareMapStore;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreWriteThroughTest extends AbstractMapStoreTest {

    @Test(timeout = 120000)
    public void testOneMemberWriteThroughWithIndex() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap<String, String> map = instance.getMap("default");
        assertEquals(0, map.size());
        assertTrue(map.tryLock("1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.get("1"));
        map.unlock("1");
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals("value2", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertTrue(map.evict("1"));
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
        testMapStore.assertAwait(1);
        assertEquals(1, testMapStore.getInitCount());
        assertEquals("default", testMapStore.getMapName());
        assertEquals(getNode(instance), getNode(testMapStore.getHazelcastInstance()));
    }

    @Test(timeout = 120000)
    public void testOneMemberWriteThroughWithLRU() {
        final int size = 10000;
        TestMapStore testMapStore = new TestMapStore(size * 2, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        MapConfig mapConfig = config.getMapConfig("default");
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
        evictionConfig.setSize(size);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Employee> map = instance.getMap("default");
        final CountDownLatch countDownLatch = new CountDownLatch(size);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
                countDownLatch.countDown();
            }
        }, false);

        for (int i = 0; i < size * 2; i++) {
            // trigger eviction
            if (i == (size * 2) - 1 || i == size) {
                sleepMillis(1001);
            }
            map.put(i, new Employee("joe", i, true, 100.00));
        }
        assertEquals(testMapStore.getStore().size(), size * 2);
        assertOpenEventually(countDownLatch);
        final String msgFailure = String.format("map size: %d put count: %d", map.size(), size);
        assertTrue(msgFailure, map.size() > size / 2);
        assertTrue(msgFailure, map.size() <= size);
        assertEquals(testMapStore.getStore().size(), size * 2);
    }

    @Test(timeout = 120000)
    public void testOneMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee newEmployee = new Employee("ali", 26, true, 1000);
        testMapStore.insert("1", employee);
        testMapStore.insert("2", employee);
        testMapStore.insert("3", employee);
        testMapStore.insert("4", employee);
        testMapStore.insert("5", employee);
        testMapStore.insert("6", employee);
        testMapStore.insert("7", employee);

        IMap<String, Employee> map = instance.getMap("default");
        map.addIndex(IndexType.HASH, "name");
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertEquals(employee, map.put("2", newEmployee));
        assertEquals(newEmployee, testMapStore.getStore().get("2"));
        assertEquals(2, map.size());

        map.remove("1");
        map.put("1", employee, 1, TimeUnit.SECONDS);
        map.put("1", employee);
        Thread.sleep(2000);
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(employee, map.get("1"));

        map.evict("2");
        assertEquals(newEmployee, map.get("2"));

        assertEquals(employee, map.get("3"));
        assertEquals(employee, map.put("3", newEmployee));
        assertEquals(newEmployee, map.get("3"));

        assertEquals(employee, map.remove("4"));

        assertEquals(employee, map.get("5"));
        assertEquals(employee, map.remove("5"));

        assertEquals(employee, map.putIfAbsent("6", newEmployee));
        assertEquals(employee, map.get("6"));
        assertEquals(employee, testMapStore.getStore().get("6"));

        assertTrue(map.containsKey("7"));
        assertEquals(employee, map.get("7"));

        assertNull(map.get("8"));
        assertFalse(map.containsKey("8"));
        assertNull(map.putIfAbsent("8", employee));
        assertEquals(employee, map.get("8"));
        assertEquals(employee, testMapStore.getStore().get("8"));
    }

    @Test(timeout = 120000)
    public void testTwoMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee employee2 = new Employee("jay", 35, false, 100.00);
        testMapStore.insert("1", employee);
        IMap<String, Employee> map = instance.getMap("default");
        map.addIndex(IndexType.HASH, "name");
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        map.put("2", employee2);
        assertEquals(employee2, testMapStore.getStore().get("2"));
        assertEquals(2, testMapStore.getStore().size());
        assertEquals(2, map.size());
        map.remove("2");
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(1, map.size());
        testMapStore.assertAwait(10);
        assertEquals(5, testMapStore.callCount.get());
    }

    @Test(timeout = 300000)
    public void testTwoMemberWriteThrough2() throws Exception {
        int items = 1000;
        TestMapStore testMapStore = new TestMapStore(items, 0, 0);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, String> map1 = h1.getMap("default");
        IMap<Integer, String> map2 = h2.getMap("default");

        for (int i = 0; i < items; i++) {
            map1.put(i, "value" + i);
        }

        assertTrue("store operations could not be done wisely ", testMapStore.latchStore.await(30, TimeUnit.SECONDS));
        assertEquals(items, testMapStore.getStore().size());
        assertEquals(items, map1.size());
        assertEquals(items, map2.size());

        testMapStore.assertAwait(10);
        // N put-load N put-store call and 1 loadAllKeys
        assertEquals(items * 2 + 1, testMapStore.callCount.get());
    }

    @Test(timeout = 120000)
    public void testOneMemberWriteThroughFailingStore() {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true, true);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map = instance.getMap("default");
        assertEquals(0, map.size());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
            ignore(e);
        }
        assertEquals(1, testMapStore.loads.get());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
            ignore(e);
        }
        assertEquals(2, testMapStore.loads.get());
        try {
            map.put("1", "value");
            fail("should have thrown exception");
        } catch (Exception e) {
            ignore(e);
        }
        assertEquals(0, testMapStore.stores.get());
        assertEquals(0, map.size());
    }

    @Test(timeout = 120000)
    public void testOneMemberWriteThroughFailingStore2() {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true, false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map = instance.getMap("default");
        assertEquals(0, map.size());

        try {
            map.put("1", "value");
            fail("should have thrown exception");
        } catch (Exception e) {
            ignore(e);
        }
        assertEquals(0, map.size());
    }
}
