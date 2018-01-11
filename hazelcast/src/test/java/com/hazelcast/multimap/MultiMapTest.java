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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapTest extends HazelcastTestSupport {

    /**
     * idGen is not set while replicating
     */
    @Test
    public void testIssue5220() {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        MultiMap<Object, Object> mm1 = instance1.getMultiMap(name);
        // populate multimap while instance1 is owner
        // records will have ids from 0 to 10
        for (int i = 0; i < 10; i++) {
            mm1.put("ping-address", "instance1-" + i);
        }
        HazelcastInstance instance2 = factory.newHazelcastInstance();
        MultiMap<Object, Object> mm2 = instance2.getMultiMap(name);
        // now the second instance is the owner
        // if idGen is not set while replicating
        // these entries will have ids from 0 to 10 too
        for (int i = 0; i < 10; i++) {
            mm2.put("ping-address", "instance2-" + i);
        }
        HazelcastInstance instance3 = factory.newHazelcastInstance();
        MultiMap<Object, Object> mm3 = instance3.getMultiMap(name);

        // since remove iterates all items and check equals it will remove correct item from owner-side
        // but for the backup we just sent the recordId. if idGen is not set while replicating
        // we may end up removing instance1's items
        for (int i = 0; i < 10; i++) {
            mm2.remove("ping-address", "instance2-" + i);
        }
        instance2.getLifecycleService().terminate();

        for (int i = 0; i < 10; i++) {
            mm1.remove("ping-address", "instance1-" + i);
        }
        instance1.shutdown();

        assertEquals(0, mm3.size());
    }

    /**
     * ConcurrentModificationExceptions
     */
    @Test
    public void testIssue1882() {
        String mmName = "mm";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final Config config = new Config();
        final MultiMapConfig multiMapConfig = config.getMultiMapConfig(mmName);
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final String key = generateKeyOwnedBy(instance1);

        final MultiMap<Object, Object> mm1 = instance1.getMultiMap("mm");
        mm1.put(key, 1);
        mm1.put(key, 2);
        final AtomicBoolean running = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                int count = 3;
                while (running.get()) {
                    mm1.put(key, count++);
                }
            }
        }.start();


        for (int i = 0; i < 10; i++) {
            mm1.get(key);
        }
    }

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
    public void testContainsKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        assertFalse(multiMap.containsKey("test"));

        multiMap.put("test", "test");
        assertTrue(multiMap.containsKey("test"));

        multiMap.remove("test");
        assertFalse(multiMap.containsKey("test"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.put(null, "someVal");
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenNullValue() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.put("someVal", null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenNullKey() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenNullKey() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsValue(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsEntry_whenNullKey() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsEntry(null, "someVal");
    }

    @Test(expected = NullPointerException.class)
    public void testContainsEntry_whenNullValue() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsEntry("someVal", null);
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
     * test localKeySet, keySet, entrySet, values, contains, containsKey and containsValue methods
     */
    @Test
    public void testCollectionInterfaceMethods() {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        getMultiMap(instances, name).put("key1", "key1_val1");
        getMultiMap(instances, name).put("key1", "key1_val2");
        getMultiMap(instances, name).put("key1", "key1_val3");

        getMultiMap(instances, name).put("key2", "key2_val1");
        getMultiMap(instances, name).put("key2", "key2_val2");

        getMultiMap(instances, name).put("key3", "key3_val1");
        getMultiMap(instances, name).put("key3", "key3_val2");
        getMultiMap(instances, name).put("key3", "key3_val3");
        getMultiMap(instances, name).put("key3", "key3_val4");

        assertTrue(getMultiMap(instances, name).containsKey("key3"));
        assertTrue(getMultiMap(instances, name).containsValue("key3_val4"));

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

        for (Object key : keySet) {
            assertContains(totalKeySet, key);
        }

        Set<Map.Entry> entrySet = getMultiMap(instances, name).entrySet();
        assertEquals(entrySet.size(), 9);
        for (Map.Entry entry : entrySet) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            assertTrue(val.startsWith(key));
        }

        Collection values = getMultiMap(instances, name).values();
        assertEquals(values.size(), 9);

        assertTrue(getMultiMap(instances, name).containsKey("key2"));
        assertFalse(getMultiMap(instances, name).containsKey("key4"));

        assertTrue(getMultiMap(instances, name).containsEntry("key3", "key3_val3"));
        assertFalse(getMultiMap(instances, name).containsEntry("key3", "key3_val7"));
        assertFalse(getMultiMap(instances, name).containsEntry("key2", "key3_val3"));

        assertTrue(getMultiMap(instances, name).containsValue("key2_val2"));
        assertFalse(getMultiMap(instances, name).containsValue("key2_val4"));
    }

    // it must throw ClassCastException wrapped by HazelcastException
    @Test(expected = HazelcastException.class)
    public void testAggregateMultiMap_differentDataTypes() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.put(1, "fail");
        multiMap.put(2, 75);

        Integer aggregate = multiMap.aggregate(Supplier.all(), Aggregations.integerAvg());

        assertEquals(50, aggregate.intValue());
    }

    @Test
    public void testAggregateMultiMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        Integer aggregate = multiMap.aggregate(Supplier.all(), Aggregations.integerAvg());
        assertEquals(0, aggregate.intValue());

        multiMap.put(1, 25);
        multiMap.put(2, 75);

        aggregate = multiMap.aggregate(Supplier.all(), Aggregations.integerAvg());
        assertEquals(50, aggregate.intValue());
    }

    private MultiMap getMultiMap(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getMultiMap(name);
    }
}
