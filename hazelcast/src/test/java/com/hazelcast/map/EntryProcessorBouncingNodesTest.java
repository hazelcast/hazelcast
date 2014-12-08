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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryProcessorBouncingNodesTest extends HazelcastTestSupport {

    private static final int ENTRIES = 10;
    private static final long ITERATIONS = 50;
    private static String MAP_NAME = "test-map";
    private TestHazelcastInstanceFactory instanceFactory;


    @Before
    public void setUp() throws Exception {
        if (instanceFactory != null) {
            instanceFactory.shutdownAll();
        }
        instanceFactory = new TestHazelcastInstanceFactory(500);
    }

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/3683
    public void testEntryProcessorWhile2NodesAreBouncing() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean isRunning = new AtomicBoolean(true);

        // start up three instances
        HazelcastInstance instance = newInstance();
        HazelcastInstance instance2 = newInstance();
        HazelcastInstance instance3 = newInstance();

        // Create a map that we'll use to test data consistency while nodes are joining and leaving the cluster
        // The basic idea is pretty simple. In a loop, for each key in the IMap, we'll add a number to a list.
        // This allows us to verify whether the numbers are added in the correct order and also whether there's
        // any data loss as nodes leave or join the cluster.
        final IMap<Integer, List<Integer>> map = instance.getMap(MAP_NAME);
        final List<Integer> expected = new ArrayList<Integer>();

        // initialize the list synchronously to ensure the map is correctly initialized
        InitListProcessor initProcessor = new InitListProcessor();
        for (int i = 0; i < ENTRIES; ++i) {
            map.executeOnKey(i, initProcessor);
        }

        assertEquals(ENTRIES, map.size());

        // spin up the threads that stop/start the instance2 and instance3, leaving one instance always running
        Thread bounceThread1 = new Thread(new TwoNodesRestartingRunnable(instance2, instance3, startLatch, isRunning));
        bounceThread1.start();

        // now, with nodes joining and leaving the cluster concurrently, start adding numbers to the lists
        int iteration = 0;
        while (iteration < ITERATIONS) {
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
        isRunning.set(false);

        // wait for the instance bounces to complete
        bounceThread1.join();


        final CountDownLatch latch = new CountDownLatch(ENTRIES);
        for (int i = 0; i < ENTRIES; ++i) {
            final int id = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    assertTrueEventually(new AssertTask() {
                        @Override
                        public void run() throws Exception {
                            assertTrue(expected.size() <= map.get(id).size());
                        }
                    });
                    latch.countDown();
                }
            }).start();
        }
        assertOpenEventually(latch);
    }

    private  HazelcastInstance newInstance() {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setBackupCount(2);
        config.addMapConfig(mapConfig);
        return instanceFactory.newHazelcastInstance(config);
    }

    private class TwoNodesRestartingRunnable implements Runnable {
        private final CountDownLatch start;
        private final AtomicBoolean isRunning;
        private HazelcastInstance instance1;
        private HazelcastInstance instance2;

        private TwoNodesRestartingRunnable(HazelcastInstance h1, HazelcastInstance h2, CountDownLatch startLatch, AtomicBoolean isRunning) {
            this.instance1 = h1;
            this.instance2 = h2;
            this.start = startLatch;
            this.isRunning = isRunning;
        }

        @Override
        public void run() {
            try {
                start.await();
                while (isRunning.get()) {
                    instance1.shutdown();
                    instance2.shutdown();
                    Thread.sleep(10l);
                    instance1 = newInstance();
                    instance2 = newInstance();
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
