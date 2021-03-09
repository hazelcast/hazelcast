/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapBackupTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testPutAllBackup() {
        int size = 100;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map1 = instance1.getMap("testPutAllBackup");
        IMap<Integer, Integer> map2 = instance2.getMap("testPutAllBackup");
        warmUpPartitions(factory.getAllHazelcastInstances());

        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }

        map2.putAll(mm);
        assertEquals(size, map2.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map2.get(i).intValue());
        }

        instance2.shutdown();
        assertEquals(size, map1.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map1.get(i).intValue());
        }
    }

    @Test
    public void testPutAllTooManyEntriesWithBackup() {
        int size = 10000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance instance2 = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map1 = instance1.getMap("testPutAllTooManyEntries");
        IMap<Integer, Integer> map2 = instance2.getMap("testPutAllTooManyEntries");
        warmUpPartitions(factory.getAllHazelcastInstances());

        Map<Integer, Integer> mm = new HashMap<Integer, Integer>();
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }

        map2.putAll(mm);
        assertEquals(size, map2.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map2.get(i).intValue());
        }

        instance2.shutdown();
        assertEquals(size, map1.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map1.get(i).intValue());
        }
    }

    @Test
    public void testIfWeCarryRecordVersionInfoToReplicas() {
        String mapName = randomMapName();
        int mapSize = 1000;
        int expectedRecordVersion = 3;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance node2 = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map1 = node1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map1.put(i, 0); // version 0
            map1.put(i, 1); // version 1
            map1.put(i, 2); // version 2
            map1.put(i, 3); // version 3
        }

        node1.shutdown();

        IMap<Integer, Integer> map3 = node2.getMap(mapName);

        for (int i = 0; i < mapSize; i++) {
            EntryView<Integer, Integer> entryView = map3.getEntryView(i);
            assertEquals(expectedRecordVersion, entryView.getVersion());
        }
    }

}
