/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RebalancingPartitionTest {

    private static final int ENTRIES = 10;
    private static final long ITERATIONS = 50;
    private static String MAP_NAME = "test-map";

    static {
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testRepartitioningCluster() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);

        System.out.println("Starting 3 Hazelcast instances");

        // start up three instances
        HazelcastInstance instance = newInstance();
        HazelcastInstance instance2 = newInstance();
        HazelcastInstance instance3 = newInstance();

        System.out.println("Stable instance: " + instance.getCluster().getLocalMember().getUuid());

        // Create a map that we'll use to test data consistency while nodes are joining and leaving the cluster
        // The basic idea is pretty simple. In a loop, for each key in the IMap, we'll add a number to a list.
        // This allows us to verify whether the numbers are added in the correct order and also whether there's
        // any data loss as nodes leave or join the cluster.
        IMap<Integer, List<Integer>> map = instance.getMap(MAP_NAME);
        List<Integer> expected = new ArrayList<Integer>();

        // initialize the list synchronously to ensure the map is correctly initialized
        InitListProcessor initProcessor = new InitListProcessor();
        for (int i = 0; i < ENTRIES; ++i) {
            map.executeOnKey(i, initProcessor);
        }

        assertEquals(ENTRIES, map.size());

        // spin up the threads that stop/start the instance2 and instance3, leaving one instance always running
        Thread bounceThread1 = new Thread(new RestartNodeRunnable(instance2, startLatch, stopLatch));
        Thread bounceThread2 = new Thread(new RestartNodeRunnable(instance3, startLatch, stopLatch));
        bounceThread1.start();
        bounceThread2.start();

        // now, with nodes joining and leaving the cluster concurrently, start adding numbers to the lists
        int iteration = 0;
        while (iteration < ITERATIONS) {
            if (iteration % 10 == 0) {
                System.out.println("iteration " + iteration);
            }
            if (iteration == 30) {
                // let the bounce threads start bouncing
                startLatch.countDown();
            }
            IncrementProcessor processor = new IncrementProcessor(iteration);
            expected.add(iteration);
            for (int i = 0; i < ENTRIES; ++i) {
                map.executeOnKey(i, processor);
            }
            // give processing time to catch up
            ++iteration;
        }

        // signal the bounce threads that we're done
        stopLatch.countDown();

        System.out.println("waiting for bounceThreads to complete");

//        // wait for the instance bounces to complete
        bounceThread1.join();
        bounceThread2.join();

        System.out.println("starting verification");
        PartitionService partitionService = instance.getPartitionService();

        List<String> errors = new ArrayList<String>();
        // validate map contents for partitions owned by instance - these have never been rebalanced
        for (int i = 0; i < ENTRIES; ++i) {
            System.out.print("verifying entry " + i);
            List<Integer> list = map.get(i);
            if (expected.size() <= list.size()) {
                // there can be one or two more entries that are inserted because of retries.
                System.out.println(" - pass");
                continue;
            }

            String owner = partitionService.getPartition(i).getOwner().getUuid();
            String error = owner + "[" + i + "] doesn't match! Expected/actual length: " + expected.size() + "/" + list.size() +
                    "\nExpected : " + expected.toString() + "\nActual   : " + list + "\n";
            System.out.println(" - fail");
            System.err.println(error);
            errors.add(error);
        }

        assertEquals(Collections.emptyList(), errors);
    }

    private static HazelcastInstance newInstance() {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setBackupCount(2);
        config.addMapConfig(mapConfig);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static class RestartNodeRunnable implements Runnable {
        private final CountDownLatch start;
        private final CountDownLatch stop;
        private HazelcastInstance instance;

        private RestartNodeRunnable(HazelcastInstance instance, CountDownLatch startLatch, CountDownLatch stopLatch) {
            this.instance = instance;
            this.start = startLatch;
            this.stop = stopLatch;
        }

        @Override
        public void run() {
            try {
                start.await();
                while (!stop.await(0, TimeUnit.MILLISECONDS)) {
                    long start = System.currentTimeMillis();
                    String name = instance.getName() + "(" + instance.getCluster().getLocalMember().getUuid() + ")";
                    instance.shutdown();
                    System.out.println(name + ": shutdown in " + (System.currentTimeMillis() - start) + " millis");
                    Thread.sleep(10l);
                    start = System.currentTimeMillis();
                    instance = newInstance();
                    String newName = instance.getName() + "(" + instance.getCluster().getLocalMember().getUuid() + ")";
                    System.out.println(name + ": restarted as " + newName + " in " + (System.currentTimeMillis() - start) + " millis");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class IncrementProcessor extends AbstractEntryProcessor<Integer, List<Integer>> {
        private final int nextVal;

        private IncrementProcessor(int nextVal) {
            this.nextVal = nextVal;
        }

        @Override
        public Object process(Map.Entry<Integer, List<Integer>> entry) {
            List<Integer> list = entry.getValue();
            if (list == null) {
                list = new ArrayList<Integer>();
            }

            list.add(nextVal);
            entry.setValue(list);

            return null;
        }
    }

    private static class InitListProcessor extends AbstractEntryProcessor<Integer, List<Integer>> {
        @Override
        public Object process(Map.Entry<Integer, List<Integer>> entry) {
            entry.setValue(new ArrayList<Integer>());
            return null;
        }
    }
}
