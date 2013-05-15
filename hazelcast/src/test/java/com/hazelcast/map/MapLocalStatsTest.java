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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(RandomBlockJUnit4ClassRunner.class)
public class MapLocalStatsTest {

    @Test
    public void test() throws InterruptedException {

        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        ObjectMapProxy map = (ObjectMapProxy) instance.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map.put(i,i);
        }
        for (int i = 0; i < 20; i++) {
            map.get(i);
        }
        for (int i = 0; i < 10; i++) {
            map.lock(i);
        }

        LocalMapStats stats = map.getLocalMapStats();

        assertEquals(1000, stats.getOwnedEntryCount());
        assertEquals(0, stats.getBackupEntryCount());
        assertEquals(20, stats.getHits());
        assertEquals(10, stats.getLockedEntryCount());

        Hazelcast.shutdownAll();
    }

    @Test
    public void testMultiNodes() throws InterruptedException {

        Config cfg = new Config();
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(cfg);
        ObjectMapProxy map1 = (ObjectMapProxy) instance1.getMap("map");
        ObjectMapProxy map2 = (ObjectMapProxy) instance2.getMap("map");

        for (int i = 0; i < 1000; i++) {
            map1.put(i, i);
        }
        for (int i = 0; i < 20; i++) {
            map1.get(i);
        }
        for (int i = 0; i < 10; i++) {
            map1.lock(i);
        }

        LocalMapStats stats1 = map1.getLocalMapStats();
        LocalMapStats stats2 = map2.getLocalMapStats();

        assertEquals(1000, stats1.getOwnedEntryCount() + stats2.getOwnedEntryCount());
        assertEquals(1000, stats1.getBackupEntryCount() + stats2.getBackupEntryCount());
        assertEquals(20, stats1.getHits() + stats2.getHits());
        assertEquals(10, stats1.getLockedEntryCount() + stats2.getLockedEntryCount());
        assertEquals(stats1.getOwnedEntryMemoryCost(), stats2.getBackupEntryMemoryCost());
        assertEquals(stats2.getOwnedEntryMemoryCost(), stats1.getBackupEntryMemoryCost());

        Hazelcast.shutdownAll();
    }



}
