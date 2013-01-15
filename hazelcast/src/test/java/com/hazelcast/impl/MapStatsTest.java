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

package com.hazelcast.impl;

import com.hazelcast.core.*;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapStatsTest {

    final String n = "foo";

    HazelcastInstance h1;
    HazelcastInstance h2;
    IMap<String, String> map1;
    IMap<String, String> map2;

    AtomicInteger globalCount = new AtomicInteger();
    AtomicInteger localCount = new AtomicInteger();
    AtomicInteger valueCount = new AtomicInteger();

    @BeforeClass
    @AfterClass
    public static void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        h1 = Hazelcast.newHazelcastInstance(null);
        h2 = Hazelcast.newHazelcastInstance(null);
        createMaps();
    }

    @After
    public void after() {
        destroyMaps();
        h1.getLifecycleService().shutdown();
        h2.getLifecycleService().shutdown();
    }

    private void createMaps() {
        globalCount.set(0);
        localCount.set(0);
        valueCount.set(0);
        map1 = h1.getMap(n);
        map2 = h2.getMap(n);
    }

    private void destroyMaps() {
        map1.destroy();
        map2.destroy();
    }

    @Test
    public void mapHitsMissesTest() {
        map1.put("key1", "value1");
        map1.put("key2", "value2");
        map1.put("key3", "value3");

        map1.get("key1");
        map1.get("key1");
        map1.get("key1");
        map1.get("key2");
        map1.get("key3");
        map1.get("keyX");
        map1.get("keyY");
        map1.get("keyZ");

        assertEquals(map1.getLocalMapStats().getHits() + map2.getLocalMapStats().getHits(), 5);
        assertEquals(map1.getLocalMapStats().getMisses() + map2.getLocalMapStats().getMisses(), 3);
        map1.get("keyA");
        map1.get("key3");
        assertEquals(map1.getLocalMapStats().getHits() + map2.getLocalMapStats().getHits(), 6);
        assertEquals(map1.getLocalMapStats().getMisses() + map2.getLocalMapStats().getMisses(), 4);
    }
    
}
