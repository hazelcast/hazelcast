/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.test;


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapTest {

    private static final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();
    private static int instanceCount = 2;
    private Random rand = new Random(Clock.currentTimeMillis());
    private static Config cfg = new Config();


    @AfterClass
    public static void shutdown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @BeforeClass
    public static void init() throws Exception {
        startInstances();
    }

    private HazelcastInstance getInstance() {
        return instances.get(rand.nextInt(instanceCount));
    }

    private HazelcastInstance getInstance(int index) {
        return instances.get(index);
    }

    private void newInstance() {
        instanceCount++;
        instances.add(Hazelcast.newHazelcastInstance(cfg));
    }

    private void newInstanceMany(int count) {
        for (int i = 0; i < count; i++) {
            instanceCount++;
            instances.add(Hazelcast.newHazelcastInstance(cfg));
        }
    }

    private void removeInstance() {
        instanceCount--;
        instances.remove(0).getLifecycleService().shutdown();
    }

    private void removeInstance(int index) {
        instanceCount--;
        instances.remove(index).getLifecycleService().shutdown();
    }

    private void removeInstanceMany(int count) {
        for (int i = 0; i < count; i++) {
            instanceCount--;
            instances.remove(0).getLifecycleService().shutdown();
        }
    }

    private void startInstances(int instanceCount) {
        MapTest.instanceCount = instanceCount;
        startInstances();
    }


    private static void startInstances() {
        Hazelcast.shutdownAll();
        instances.clear();
        for (int i = 0; i < instanceCount; i++) {
            instances.add(Hazelcast.newHazelcastInstance(cfg));
        }
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = getInstance().getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
        value = map.put("Hello", "New World");
        assertEquals("World", value);
        assertEquals("New World", map.get("Hello"));
    }

    @Test
    public void testMapPutIfAbsent() {
        IMap<String, String> map = getInstance().getMap("testMapPutIfAbsent");
        assertEquals(map.putIfAbsent("key1", "value1"), null);
        System.out.println(map.get("key1"));
        assertEquals(map.putIfAbsent("key2", "value2"), null);
        assertEquals(map.putIfAbsent("key1", "valueX"), "value1");
        assertEquals(map.get("key1"), "value1");
        assertEquals(map.size(), 2);
    }


    @Test
    public void testMapRemove() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.remove("key1"), "value1");
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key1"), null);
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key3"), "value3");
        assertEquals(map.size(), 1);
    }


    @Test
    public void testMapSet() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.set("key1", "valueX", 0, TimeUnit.MILLISECONDS);
        map.set("key2", "value2", 0, TimeUnit.MILLISECONDS);
        assertEquals(map.size(), 2);
        assertEquals(map.get("key1"), "valueX");
        assertEquals(map.get("key2"), "value2");
    }


    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.containsKey("key1"), true);
        assertEquals(map.containsKey("key5"), false);
        map.remove("key1");
        assertEquals(map.containsKey("key1"), false);
        assertEquals(map.containsKey("key2"), true);
        assertEquals(map.containsKey("key5"), false);
    }


    @Test
    public void testGetPutAndSizeWhileStartShutdown() {
        IMap<String, String> map = getInstance().getMap("testGetPutAndSizeWhileStartShutdown");
        try {
            for (int i = 1; i < 10000; i++) {
                map.put("key" + i, "value" + i);
                if (i == 100) {
                    new Thread(new Runnable() {
                        public void run() {
                            newInstanceMany(2);
                        }
                    }).start();
                }

                if (i == 600) {
                    new Thread(new Runnable() {
                        public void run() {
                            removeInstance();
                        }
                    }).start();
                }
                Thread.sleep(5);
            }
            Thread.sleep(3000);
            assertEquals(map.size(), 10000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }


}
