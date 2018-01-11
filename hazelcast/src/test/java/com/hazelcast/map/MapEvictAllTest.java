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

package com.hazelcast.map;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapEvictAllTest extends HazelcastTestSupport {

    @Test
    public void testEvictAll_firesEvent() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1000);
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testEvictAll_preserveLockedKeys() throws Exception {
        int numberOfEntries = 1000;
        int numberOfLockedKeys = 123;
        int expectedNumberOfEvictedKeys = numberOfEntries - numberOfLockedKeys;
        final CountDownLatch countDownLatch = new CountDownLatch(expectedNumberOfEvictedKeys);
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < numberOfEntries; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < numberOfLockedKeys; i++) {
            map.lock(i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
        assertEquals(numberOfLockedKeys, map.size());
    }

    @Test
    public void testEvictAll_onBackup() throws Exception {
        int numberOfEntries = 10000;
        String mapName = randomMapName();
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfEntries);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = instanceFactory.newHazelcastInstance(getConfig());
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(getConfig());
        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);
        map1.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        }, false);

        for (int i = 0; i < numberOfEntries; i++) {
            map1.put(i, i);
        }
        map1.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, map1.getLocalMapStats().getHeapCost());
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, map2.getLocalMapStats().getHeapCost());
            }
        });
    }
}
