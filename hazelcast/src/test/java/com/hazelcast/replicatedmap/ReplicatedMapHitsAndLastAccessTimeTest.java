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
import com.hazelcast.config.ReplicatedMapConfig;
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

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapHitsAndLastAccessTimeTest extends ReplicatedMapBaseTest {

    @Test
    public void testHitsAndLastAccessTimeObjectDelay0() throws Exception {
        testHitsAndLastAccessTime(buildConfig(InMemoryFormat.OBJECT, 0));
    }

    @Test
    public void testHitsAndLastAccessTimeObjectDelayDefault() throws Exception {
        testHitsAndLastAccessTime(buildConfig(InMemoryFormat.OBJECT, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    @Test
    public void testHitsAndLastAccessTimeBinaryDelay0() throws Exception {
        testHitsAndLastAccessTime(buildConfig(InMemoryFormat.BINARY, 0));
    }

    @Test
    public void testHitsAndLastAccessTimeBinaryDelayDefault() throws Exception {
        testHitsAndLastAccessTime(buildConfig(InMemoryFormat.BINARY, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS));
    }

    private void testHitsAndLastAccessTime(Config config) throws Exception {
        long startTime = Clock.currentTimeMillis();

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 0.75, map1, map2);

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertRecord(getReplicatedRecord(map1, entry.getKey()), startTime);
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertRecord(getReplicatedRecord(map2, entry.getKey()), startTime);
        }
    }

    private void assertRecord(ReplicatedRecord<String, String> record, long startTime) {
        long hits = record.getHits();
        long lastAccessTime = record.getLastAccessTime();
        long now = Clock.currentTimeMillis();
        assertTrue(
                String.format("Hits should be greater than 0: %d > %d", hits, 0),
                hits > 0
        );
        assertTrue(
                String.format("Hits should be less than 1000: %d < %d", hits, 1000),
                hits < 1000
        );
        assertTrue(
                String.format("LastAccessTime should be greater than startTime: %d > %d", lastAccessTime, startTime),
                lastAccessTime > startTime
        );
        assertTrue(
                String.format("LastAccessTime should be less or equal than current time: %d <= %d", lastAccessTime, now),
                lastAccessTime <= now
        );
    }
}