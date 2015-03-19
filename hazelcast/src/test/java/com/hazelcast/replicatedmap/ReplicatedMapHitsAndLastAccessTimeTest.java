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
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.config.ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapHitsAndLastAccessTimeTest
        extends ReplicatedMapBaseTest {

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_objectDelay0()
            throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_objectDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(
                buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_BinaryDelay0()
            throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeSetToAnyValueAfterStartTime_binaryDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(
                buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAndLastAccessTimeIsSetToAnyValueAfterStartTime(Config config)
            throws Exception {
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
    public void test_hitsAndAccessTimeAreZeroInitially_withSingleNode_objectDelay0()
            throws Exception {
        testHitsAndAccessTimeAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAndAccessTimeAreZeroInitially_withSingleNode_objectDelayDefault()
            throws Exception {
        testHitsAndAccessTimeAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAndAccessTimeAreZeroInitially_withSingleNode_BinaryDelay0()
            throws Exception {
        testHitsAndAccessTimeAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void test_hitsAndAccessTimeAreZeroInitially_withSingleNode_binaryDelayDefault()
            throws Exception {
        testHitsAndAccessTimeAreZeroInitiallyWithSingleNode(buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAndAccessTimeAreZeroInitiallyWithSingleNode(Config config)
            throws Exception {
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
            assertEquals(0, replicatedRecord.getLastAccessTime());
        }
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_objectDelay0()
            throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_objectDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_BinaryDelay0()
            throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_withSingleNode_binaryDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeAreSetWithSingleNode(buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAndLastAccessTimeAreSetWithSingleNode(Config config)
            throws Exception {
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
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_objectDelay0()
            throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_objectDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_BinaryDelay0()
            throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void test_hitsAndLastAccessTimeAreSet_with2Nodes_binaryDelayDefault()
            throws Exception {
        testHitsAndLastAccessTimeAreSetFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAndLastAccessTimeAreSetFor1Of2Nodes(Config config)
            throws Exception {
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
            assertEquals(0, replicatedRecord.getLastAccessTime());
        }
    }

    private void execute(final Runnable runnable, final EntryEventType type, final int operations, final double minExpectation, ReplicatedMap... maps)
            throws TimeoutException {
        final WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(runnable, 60, type, operations, minExpectation, maps);
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_objectDelay0()
            throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_objectDelayDefault()
            throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_BinaryDelay0()
            throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.BINARY, 0));
    }


    @Test
    public void test_hitsAreIncrementedOnPuts_withSingleNode_binaryDelayDefault()
            throws Exception {
        testHitsAreIncrementedOnPutsWithSingleNode(buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAreIncrementedOnPutsWithSingleNode(final Config config)
            throws Exception {
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
    public void test_hitsAreIncrementedOnPuts_with2Nodes_objectDelay0()
            throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_objectDelayDefault()
            throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_BinaryDelay0()
            throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_binaryDelayDefault()
            throws Exception {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY, DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAreIncrementedOnPutsFor1Of2Nodes(final Config config)
            throws Exception {
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
