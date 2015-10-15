/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapHitsAndLastAccessTimeTest extends ReplicatedMapBaseTest {

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_object() throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_Binary() throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(Config config) throws Exception {
        final long startTime = Clock.currentTimeMillis();

        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final String mapName = randomMapName();
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);

        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put(i, i);
                }
            }
        }, ADDED, operations, 0.75, map1, map2);

        for (Map.Entry<Integer, Integer> entry : map1.entrySet()) {
            assertRecord(getReplicatedRecord(map1, entry.getKey()), startTime);
        }

        for (Map.Entry<Integer, Integer> entry : map2.entrySet()) {
            assertRecord(getReplicatedRecord(map2, entry.getKey()), startTime);
        }
    }

    private void assertRecord(ReplicatedRecord<Integer, Integer> record, long startTime) {
        long hits = record.getHits();
        long lastAccessTime = record.getLastAccessTime();
        long now = Clock.currentTimeMillis();
        assertTrue(String.format("Hits should be greater than 0: %d > %d", hits, 0), hits > 0);
        assertTrue(String.format("Hits should be less than 1000: %d < %d", hits, 1000), hits < 1000);
        assertTrue(String.format("LastAccessTime should be greater than startTime: %d > %d", lastAccessTime, startTime),
                lastAccessTime > startTime);
        assertTrue(String.format("LastAccessTime should be less or equal than current time: %d <= %d", lastAccessTime, now),
                lastAccessTime <= now);
    }

    @Test
    public void test_hitsAreZeroInitially_withSingleNode_object() throws Exception {
        testHitsAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAreZeroInitially_withSingleNode_Binary() throws Exception {
        testHitsAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAreZeroInitiallyWithSingleNode(Config config) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map = instance1.getReplicatedMap(randomMapName());
        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map.put(i, i);
                }
            }
        }, ADDED, operations, 1, map);

        for (int i = 0; i < operations; i++) {
            final ReplicatedRecord<Integer, Integer> replicatedRecord = getReplicatedRecord(map, i);
            assertEquals(0, replicatedRecord.getHits());
        }
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_object() throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_Binary() throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAndLastAccessTimeAreSetWithSingleNode(Config config) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map = instance1.getReplicatedMap(randomMapName());
        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map.put(i, i);
                }
            }
        }, ADDED, operations, 1, map);

        for (int i = 0; i < operations; i++) {
            map.containsKey(i);
        }

        for (int i = 0; i < operations; i++) {
            final ReplicatedRecord<Integer, Integer> replicatedRecord = getReplicatedRecord(map, i);
            assertEquals(1, replicatedRecord.getHits());
            assertTrue("Last access time should be set for " + i, replicatedRecord.getLastAccessTime() > 0);
        }
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_object() throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_Binary() throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAndLastAccessTimeAreSetFor1Of2Nodes(Config config) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final String mapName = randomMapName();
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);
        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put(i, i);
                    map1.containsKey(i);
                }
            }
        }, ADDED, operations, 1, map1, map2);

        for (int i = 0; i < operations; i++) {
            final ReplicatedRecord<Integer, Integer> replicatedRecord = getReplicatedRecord(map1, i);
            assertEquals(1, replicatedRecord.getHits());
            assertTrue("Last access time should be set for " + i, replicatedRecord.getLastAccessTime() > 0);
        }

        for (int i = 0; i < operations; i++) {
            final ReplicatedRecord<Integer, Integer> replicatedRecord = getReplicatedRecord(map2, i);
            assertEquals(0, replicatedRecord.getHits());
            assertTrue("Last access time should be set for " + i, replicatedRecord.getLastAccessTime() > 0);
        }
    }

    private void execute(final Runnable runnable, final EntryEventType type, final int operations,
                         final double minExpectation, ReplicatedMap... maps) throws TimeoutException {
        final WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(runnable, 60, type, operations, minExpectation, maps);
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_object() throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_Binary() throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.BINARY));
    }


    private void testHitsAreIncrementedOnPutsWithSingleNode(final Config config) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map = instance1.getReplicatedMap(randomMapName());
        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map.put(i, i);
                }
            }
        }, ADDED, operations, 1, map);

        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map.put(i, i);
                }
            }
        }, UPDATED, operations, 1, map);

        for (int i = 0; i < operations; i++) {
            assertEquals(1, getReplicatedRecord(map, i).getHits());
        }
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_object() throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_Binary() throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAreIncrementedOnPutsFor1Of2Nodes(final Config config) throws Exception {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final String mapName = randomMapName();
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);
        final int operations = 100;
        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put(i, i);
                }
            }
        }, ADDED, operations, 1, map1, map2);

        execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put(i, i + 1);
                }
            }
        }, UPDATED, operations, 1, map1, map2);

        for (int i = 0; i < operations; i++) {
            assertEquals(1, getReplicatedRecord(map1, i).getHits());
            assertEquals(0, getReplicatedRecord(map2, i).getHits());
        }
    }
}
