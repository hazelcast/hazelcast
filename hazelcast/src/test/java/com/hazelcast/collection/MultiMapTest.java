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

package com.hazelcast.collection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @ali 1/17/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class MultiMapTest extends HazelcastTestSupport {

    @Test
    public void testPutGetRemoveWhileCollectionTypeSet() throws InterruptedException {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);

        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        assertTrue(getMultiMap(instances, name).put("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).put("key1", "key1_value2"));

        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));
        assertFalse(getMultiMap(instances, name).put("key2", "key2_value1"));

        assertEquals(getMultiMap(instances, name).valueCount("key1"), 2);
        assertEquals(getMultiMap(instances, name).valueCount("key2"), 1);
        assertEquals(getMultiMap(instances, name).size(), 3);

        Collection coll = getMultiMap(instances, name).get("key2");
        assertEquals(coll.size(), 1);
        Iterator iter = coll.iterator();
        Object o = iter.next();
        assertEquals(o, "key2_value1");

        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertFalse(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value2"));


        coll = getMultiMap(instances, name).get("key1");
        assertEquals(coll.size(), 0);

        coll = getMultiMap(instances, name).remove("key2");
        assertEquals(coll.size(), 1);
        iter = coll.iterator();
        o = iter.next();
        assertEquals(o, "key2_value1");

    }

    @Test
    public void testPutGetRemoveWhileCollectionTypeList() throws InterruptedException {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        assertTrue(getMultiMap(instances, name).put("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).put("key1", "key1_value2"));


        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));
        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));

        assertEquals(getMultiMap(instances, name).valueCount("key1"), 2);
        assertEquals(getMultiMap(instances, name).valueCount("key2"), 2);
        assertEquals(getMultiMap(instances, name).size(), 4);

        Collection coll = getMultiMap(instances, name).get("key1");
        assertEquals(coll.size(), 2);
        Iterator iter = coll.iterator();
        assertEquals(iter.next(), "key1_value1");
        assertEquals(iter.next(), "key1_value2");

        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertFalse(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value2"));

        coll = getMultiMap(instances, name).get("key1");
        assertEquals(coll.size(), 0);

        coll = getMultiMap(instances, name).remove("key2");
        assertEquals(coll.size(), 2);
        iter = coll.iterator();
        assertEquals(iter.next(), "key2_value1");
        assertEquals(iter.next(), "key2_value1");
    }

    /**
     * test localKeySet, keySet, entrySet, values and contains methods
     */
    @Test
    public void testCollectionInterfaceMethods(){
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        getMultiMap(instances, name).put("key1", "key1_val1");
        getMultiMap(instances, name).put("key1","key1_val2");
        getMultiMap(instances, name).put("key1","key1_val3");

        getMultiMap(instances, name).put("key2","key2_val1");
        getMultiMap(instances, name).put("key2","key2_val2");

        getMultiMap(instances, name).put("key3","key3_val1");
        getMultiMap(instances, name).put("key3","key3_val2");
        getMultiMap(instances, name).put("key3","key3_val3");
        getMultiMap(instances, name).put("key3","key3_val4");


        Set totalKeySet = new HashSet();
        Set localKeySet = instances[0].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);

        localKeySet = instances[1].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);

        localKeySet = instances[2].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);

        localKeySet = instances[3].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);
        assertEquals(3, totalKeySet.size());

        Set keySet = getMultiMap(instances, name).keySet();
        assertEquals(keySet.size(), 3);

        for (Object key: keySet){
            assertTrue(totalKeySet.contains(key));
        }

        Set<Map.Entry> entrySet = getMultiMap(instances, name).entrySet();
        assertEquals(entrySet.size(), 9);
        for (Map.Entry entry: entrySet){
            String key = (String)entry.getKey();
            String val = (String)entry.getValue();
            assertTrue(val.startsWith(key));
        }

        Collection values = getMultiMap(instances, name).values();
        assertEquals(values.size(), 9);

        assertTrue(getMultiMap(instances, name).containsKey("key2"));
        assertFalse(getMultiMap(instances, name).containsKey("key4"));

        assertTrue(getMultiMap(instances, name).containsEntry("key3","key3_val3"));
        assertFalse(getMultiMap(instances, name).containsEntry("key3","key3_val7"));
        assertFalse(getMultiMap(instances, name).containsEntry("key2","key3_val3"));

        assertTrue(getMultiMap(instances, name).containsValue("key2_val2"));
        assertFalse(getMultiMap(instances, name).containsValue("key2_val4"));
    }

    @Test
    public void testListeners() throws Exception {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final Set keys = new HashSet();

        EntryListener listener = new EntryListener() {
            public void entryAdded(EntryEvent event) {
                keys.add(event.getKey());
            }

            public void entryRemoved(EntryEvent event) {
                keys.remove(event.getKey());
            }

            public void entryUpdated(EntryEvent event) {
            }

            public void entryEvicted(EntryEvent event) {
            }
        };

        final String id = instances[0].getMultiMap(name).addLocalEntryListener(listener);
        instances[0].getMultiMap(name).put("key1","val1");
        instances[0].getMultiMap(name).put("key2","val2");
        instances[0].getMultiMap(name).put("key3","val3");
        instances[0].getMultiMap(name).put("key4","val4");
        instances[0].getMultiMap(name).put("key5","val5");
        instances[0].getMultiMap(name).put("key6","val6");
        instances[0].getMultiMap(name).put("key7","val7");
        instances[0].getMultiMap(name).put("key8","val8");
        Thread.sleep(1500);
        assertTrue(instances[0].getMultiMap(name).localKeySet().containsAll(keys));
        if (keys.size() != 0){
            instances[0].getMultiMap(name).remove(keys.iterator().next());
        }
        Thread.sleep(1500);
        assertTrue(instances[0].getMultiMap(name).localKeySet().containsAll(keys));
        instances[0].getMultiMap(name).removeEntryListener(id);
        getMultiMap(instances, name).clear();
        keys.clear();

        final String id2 = instances[0].getMultiMap(name).addEntryListener(listener, true);
        getMultiMap(instances, name).put("key3","val3");
        getMultiMap(instances, name).put("key3","val33");
        getMultiMap(instances, name).put("key4","val4");
        getMultiMap(instances, name).remove("key3","val33");
        Thread.sleep(1500);
        assertEquals(1, keys.size());
        getMultiMap(instances, name).clear();
        Thread.sleep(1500);
        assertEquals(0, keys.size());

        instances[0].getMultiMap(name).removeEntryListener(id2);
        instances[0].getMultiMap(name).addEntryListener(listener, "key7", true);
        getMultiMap(instances, name).put("key2","val2");
        getMultiMap(instances, name).put("key3","val3");
        getMultiMap(instances, name).put("key7","val7");
        Thread.sleep(1500);
        assertEquals(1, keys.size());
    }

    @Test
    public void testLock() throws Exception {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(){
            public void run() {
                instances[0].getMultiMap(name).lock("alo");
                latch.countDown();
                try {
                    latch2.await(10, TimeUnit.SECONDS);
                    instances[0].getMultiMap(name).unlock("alo");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertFalse(getMultiMap(instances, name).tryLock("alo"));
        latch2.countDown();
        assertTrue(instances[0].getMultiMap(name).tryLock("alo", 20, TimeUnit.SECONDS));

        new Thread(){
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                instances[0].getLifecycleService().shutdown();
            }
        }.start();

        assertTrue(instances[1].getMultiMap(name).tryLock("alo", 20, TimeUnit.SECONDS));

    }

    private MultiMap getMultiMap(HazelcastInstance[] instances, String name){
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getMultiMap(name);
    }
}
