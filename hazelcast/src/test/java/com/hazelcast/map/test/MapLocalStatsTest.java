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

package com.hazelcast.map.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.monitor.LocalMapStats;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
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

}
