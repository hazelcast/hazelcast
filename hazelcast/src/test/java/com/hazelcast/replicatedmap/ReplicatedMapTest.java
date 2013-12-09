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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapTest extends HazelcastTestSupport {

    private static final Comparator<Entry<Integer, Integer>> ENTRYSET_COMPARATOR = new Comparator<Entry<Integer, Integer>>() {
        @Override
        public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    @Test
    public void testAddObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map2.put("bar", "foo");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testUpdateObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.put("foo", "bar2");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        map2.put("foo", "bar3");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testRemoveObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.remove("foo");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testValuesObject() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
            valuesTestValues.add(entry.getValue());
        }
        TimeUnit.SECONDS.sleep(2);

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
            keySetTestValues.add(entry.getKey());
        }
        TimeUnit.SECONDS.sleep(2);

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }
        TimeUnit.SECONDS.sleep(2);

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map2.put("bar", "foo");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testUpdateBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.put("foo", "bar2");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        map2.put("foo", "bar3");
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testRemoveBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        map1.put("foo", "bar");
        TimeUnit.SECONDS.sleep(2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        map1.remove("foo");
        TimeUnit.SECONDS.sleep(2);

        value = map2.get("foo");
        assertNull(value);
    }


    @Test
    public void testValuesBinary() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
            valuesTestValues.add(entry.getValue());
        }
        TimeUnit.SECONDS.sleep(2);

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
            keySetTestValues.add(entry.getKey());
        }
        TimeUnit.SECONDS.sleep(2);

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
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap map = i < half ? map1 : map2;
            SimpleEntry entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }
        TimeUnit.SECONDS.sleep(2);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    private SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random(-System.currentTimeMillis());
        SimpleEntry<Integer, Integer>[] testValues = new SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }

}
