/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapHitsAndLastAccessTimeTest extends ReplicatedMapAbstractTest {

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
        warmUpPartitions(instance1, instance2);

        final String mapName = randomMapName();
        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap(mapName);

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Map.Entry<String, String> entry : map1.entrySet()) {
                    assertRecord(getReplicatedRecord(map1, entry.getKey()), startTime);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Map.Entry<String, String> entry : map2.entrySet()) {
                    assertRecord(getReplicatedRecord(map2, entry.getKey()), startTime);
                }
            }
        });
    }

    private void assertRecord(ReplicatedRecord<String, String> record, long startTime) {
        assertNotNull(record);
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

        final ReplicatedMap<String, String> map = instance1.getReplicatedMap(randomMapName());
        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map.put(key, "bar");
        }

        for (String key : keys) {
            final ReplicatedRecord<String, String> replicatedRecord = getReplicatedRecord(map, key);
            assertNotNull(replicatedRecord);
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

        final ReplicatedMap<String, String> map = instance1.getReplicatedMap(randomMapName());
        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map.put(key, "bar");
        }

        for (String key : keys) {
            map.containsKey(key);
        }

        for (String key : keys) {
            final ReplicatedRecord<String, String> replicatedRecord = getReplicatedRecord(map, key);
            assertNotNull(replicatedRecord);
            assertEquals(1, replicatedRecord.getHits());
            assertTrue("Last access time should be set for " + key, replicatedRecord.getLastAccessTime() > 0);
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
        warmUpPartitions(instance1, instance2);

        final String mapName = randomMapName();
        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap(mapName);
        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
            map1.containsKey(key);
        }

        for (String key : keys) {
            final ReplicatedRecord<String, String> replicatedRecord = getReplicatedRecord(map1, key);
            assertNotNull(replicatedRecord);
            assertEquals(1, replicatedRecord.getHits());
            assertTrue("Last access time should be set for " + key, replicatedRecord.getLastAccessTime() > 0);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (String key : keys) {
                    final ReplicatedRecord<String, String> replicatedRecord = getReplicatedRecord(map2, key);
                    assertNotNull(replicatedRecord);
                    assertEquals(0, replicatedRecord.getHits());
                    assertTrue("Last access time should be set for " + key, replicatedRecord.getLastAccessTime() > 0);
                }
            }
        });
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

        final ReplicatedMap<String, String> map = instance1.getReplicatedMap(randomMapName());
        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map.put(key, "bar");
        }

        for (String key : keys) {
            map.put(key, "bar");
        }

        for (String key : keys) {
            final ReplicatedRecord<String, String> record = getReplicatedRecord(map, key);
            assertNotNull(record);
            assertEquals(1, record.getHits());
        }
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_object() {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void test_hitsAreIncrementedOnPuts_with2Nodes_Binary() {
        testHitsAreIncrementedOnPutsFor1Of2Nodes(buildConfig(InMemoryFormat.BINARY));
    }

    private void testHitsAreIncrementedOnPutsFor1Of2Nodes(final Config config) {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        warmUpPartitions(instance1, instance2);

        final String mapName = randomMapName();
        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap(mapName);

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (String key : keys) {
                    final ReplicatedRecord<String, String> record1 = getReplicatedRecord(map1, key);
                    assertNotNull(record1);
                    assertEquals(1, record1.getHits());
                    final ReplicatedRecord<String, String> record2 = getReplicatedRecord(map2, key);
                    assertNotNull(record2);
                    assertEquals(0, record2.getHits());
                }
            }
        });
    }
}
