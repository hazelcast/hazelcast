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

package com.hazelcast.map;

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration.DriverType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapPartitionIteratorBouncingTest extends HazelcastTestSupport {

    private final Logger logger = Logger.getLogger(getClass().getName());
    private static final String TEST_MAP_NAME = "testMap";
    private static final int STABLE_ENTRY_COUNT = 10000;
    private static final int CONCURRENCY = 2;
    public static final int FETCH_SIZE = 100;
    public static final int MUTATION_ENTRY_FACTOR = 10;
    public AtomicInteger successfullIterations = new AtomicInteger();

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(getConfig())
                            .clusterSize(4)
                            .driverCount(4)
                            .driverType(isClientDriver() ? DriverType.CLIENT : DriverType.MEMBER)
                            .build();

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void test() {
        IMap<Integer, Integer> map = bounceMemberRule.getSteadyMember().getMap(TEST_MAP_NAME);
        populateMap(map);

        Runnable[] testTasks = new Runnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; ) {
            HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
            testTasks[i++] = new IterationRunnable(driver);
            testTasks[i++] = new MutationRunnable(driver, i / 2);
        }
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    private void populateMap(IMap<Integer, Integer> map) {
        for (int i = 0; i < STABLE_ENTRY_COUNT; i++) {
            map.put(i, i);
        }
    }

    public class IterationRunnable implements Runnable {

        private final HazelcastInstance hazelcastInstance;
        private IMap<Integer, Integer> map;

        public IterationRunnable(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hazelcastInstance.getMap(TEST_MAP_NAME);
            }
            HashSet<Integer> all = getAll();
            for (int i = 0; i < STABLE_ENTRY_COUNT; i++) {
                assertTrue("Missing stable entry", all.contains(i));
            }

            logger.info("Successfully finished iteration " + successfullIterations.incrementAndGet());
        }

        private HashSet<Integer> getAll() {
            HashSet<Integer> keys = new HashSet<>();
            int partitionCount = hazelcastInstance.getPartitionService().getPartitions().size();
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                Iterator<Entry<Integer, Integer>> iterator = createIterator(map, FETCH_SIZE, partitionId, false);
                while (iterator.hasNext()) {
                    Entry<Integer, Integer> e = iterator.next();
                    assertTrue("Got the same key twice", keys.add(e.getKey()));
                }
            }
            return keys;
        }

    }

    private Iterator<Entry<Integer, Integer>> createIterator(
            IMap<Integer, Integer> map, int fetchSize, int partitionId, boolean prefetchValues) {
        return isClientDriver()
                ? ((ClientMapProxy<Integer, Integer>) map).iterator(fetchSize, partitionId, prefetchValues)
                : ((MapProxyImpl<Integer, Integer>) map).iterator(fetchSize, partitionId, prefetchValues);
    }

    protected boolean isClientDriver() {
        return false;
    }

    public class MutationRunnable implements Runnable {
        private final HazelcastInstance hazelcastInstance;
        private final int startIndex;
        private final int endIndex;
        private IMap<Integer, Integer> map;

        public MutationRunnable(HazelcastInstance hazelcastInstance, int runnableIndex) {
            this.hazelcastInstance = hazelcastInstance;
            this.startIndex = runnableIndex * MUTATION_ENTRY_FACTOR * STABLE_ENTRY_COUNT;
            this.endIndex = startIndex + MUTATION_ENTRY_FACTOR * STABLE_ENTRY_COUNT;
        }

        @Override
        public void run() {
            if (map == null) {
                map = hazelcastInstance.getMap(TEST_MAP_NAME);
            }

            for (int i = startIndex; i < endIndex; i++) {
                map.put(i, i);
            }

            for (int i = startIndex; i < endIndex; i++) {
                map.remove(i, i);
            }
        }
    }
}
