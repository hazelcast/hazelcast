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

package com.hazelcast.replicatedmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ReplicatedMapTtlTest extends ReplicatedMapAbstractTest {

    @Test
    public void testPutWithTTL_withMigration() {
        int nodeCount = 1;
        int keyCount = 10000;
        int operationCount = 10000;
        int threadCount = 15;
        int ttl = 500;
        testPutWithTTL(nodeCount, keyCount, operationCount, threadCount, ttl, true);
    }

    @Test
    public void testPutWithTTL_withoutMigration() {
        int nodeCount = 5;
        int keyCount = 10000;
        int operationCount = 10000;
        int threadCount = 10;
        int ttl = 500;
        testPutWithTTL(nodeCount, keyCount, operationCount, threadCount, ttl, false);
    }

    private void testPutWithTTL(int nodeCount, int keyCount, int operationCount, int threadCount, int ttl,
                                boolean causeMigration) {
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(null, nodeCount);
        String mapName = randomMapName();
        List<ReplicatedMap<String, Object>> maps = createMapOnEachInstance(instances, mapName);
        ArrayList<Integer> keys = generateRandomIntegerList(keyCount);
        Thread[] threads = createThreads(threadCount, maps, keys, ttl, timeUnit, operationCount);
        for (Thread thread : threads) {
            thread.start();
        }
        HazelcastInstance instance = null;
        if (causeMigration) {
            instance = factory.newHazelcastInstance();
        }
        assertJoinable(threads);
        if (causeMigration) {
            ReplicatedMap<String, Object> map = instance.getReplicatedMap(mapName);
            maps.add(map);
        }
        for (ReplicatedMap map : maps) {
            assertSizeEventually(0, map, 60);
        }
    }

    private Thread[] createThreads(int count, List<ReplicatedMap<String, Object>> maps, ArrayList<Integer> keys,
                                   long ttl, TimeUnit timeunit, int operations) {
        Thread[] threads = new Thread[count];
        int size = maps.size();
        for (int i = 0; i < count; i++) {
            threads[i] = createPutOperationThread(maps.get(i % size), keys, ttl, timeunit, operations);
        }
        return threads;
    }

    private Thread createPutOperationThread(final ReplicatedMap<String, Object> map, final ArrayList<Integer> keys,
                                            final long ttl, final TimeUnit timeunit, final int operations) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                int size = keys.size();
                for (int i = 0; i < operations; i++) {
                    int index = i % size;
                    String key = "foo-" + keys.get(index);
                    map.put(key, random.nextLong(), 1 + random.nextInt((int) ttl), timeunit);
                }
            }
        });
    }
}
