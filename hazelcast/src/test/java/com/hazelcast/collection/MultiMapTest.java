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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @ali 1/17/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MultiMapTest {

    @BeforeClass
    public static void init() {
//        System.setProperty("hazelcast.test.use.network","true");
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPutGetRemoveWhileCollectionTypeSet() throws InterruptedException {
        Config config = new Config();
        final String name = "defMM";
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        final int count = 100;

        final int insCount = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);

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
        final int count = 100;
        final int insCount = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);

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
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);

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

    private MultiMap getMultiMap(HazelcastInstance[] instances, String name){
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getMultiMap(name);
    }
}
