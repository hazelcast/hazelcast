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

package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * This tests verifies that map updates are not lost. So we have a client which is going to do updates on a map
 * and in the end we verify that the actual updates in the map, are the same as the expected updates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapUpdateStressTest extends StressTestSupport {

    public static final int CLIENT_THREAD_COUNT = 5;
    public static final int MAP_SIZE = 100 * 1000;

    private HazelcastInstance client;
    private IMap<Integer, Integer> map;
    private StressThread[] stressThreads;

    @Before
    public void setUp() {
        super.setUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        map = client.getMap("map");

        stressThreads = new StressThread[CLIENT_THREAD_COUNT];
        for (int k = 0; k < stressThreads.length; k++) {
            stressThreads[k] = new StressThread();
            stressThreads[k].start();
        }
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        super.tearDown();
    }

    @Test
    @Ignore
    public void testChangingCluster() {
        test(true);
    }

    @Test(timeout = 600000)
    public void testFixedCluster() {
        test(false);
    }

    public void test(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);
        fillMap();
        startAndWaitForTestCompletion();
        joinAll(stressThreads);
        assertNoUpdateFailures();
    }

    private void assertNoUpdateFailures() {
        int[] increments = new int[MAP_SIZE];
        for (StressThread t : stressThreads) {
            t.addIncrements(increments);
        }

        Set<Integer> failedKeys = new HashSet<Integer>();
        for (int k = 0; k < MAP_SIZE; k++) {
            int expectedValue = increments[k];
            int foundValue = map.get(k);
            if (expectedValue != foundValue) {
                failedKeys.add(k);
            }
        }

        if (failedKeys.isEmpty()) {
            return;
        }

        int index = 1;
        for (Integer key : failedKeys) {
            System.err.println("Failed write: " + index + " found:" + map.get(key) + " expected:" + increments[key]);
            index++;
        }

        fail("There are failed writes, number of failures:" + failedKeys.size());
    }

    private void fillMap() {
        System.out.println("==================================================================");
        System.out.println("Inserting data in map");
        System.out.println("==================================================================");

        for (int k = 0; k < MAP_SIZE; k++) {
            map.put(k, 0);
            if (k % 10000 == 0) {
                System.out.println("Inserted data: " + k);
            }
        }

        System.out.println("==================================================================");
        System.out.println("Completed with inserting data in map");
        System.out.println("==================================================================");
    }

    public class StressThread extends TestThread {

        private final int[] increments = new int[MAP_SIZE];

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int key = random.nextInt(MAP_SIZE);
                int increment = random.nextInt(10);
                increments[key] += increment;
                for (; ; ) {
                    int oldValue = map.get(key);
                    if (map.replace(key, oldValue, oldValue + increment)) {
                        break;
                    }
                }
            }
        }

        public void addIncrements(int[] increments) {
            for (int k = 0; k < increments.length; k++) {
                increments[k] += this.increments[k];
            }
        }
    }
}
