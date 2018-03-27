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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Creates a map that is used to test data consistency while nodes are joining and leaving the cluster.
 * <p>
 * The basic idea is pretty simple. We'll add a number to a list for each key in the IMap. This allows us to verify whether
 * the numbers are added in the correct order and also whether there's any data loss as nodes leave or join the cluster.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryProcessorBouncingNodesTest extends HazelcastTestSupport {

    private static final int ENTRIES = 10;
    private static final int ITERATIONS = 50;
    private static final String MAP_NAME = "entryProcessorBouncingNodesTestMap";

    private TestHazelcastInstanceFactory instanceFactory;

    @Before
    public void setUp() {
        instanceFactory = new TestHazelcastInstanceFactory(500);
    }

    @After
    public void tearDown() {
        instanceFactory.shutdownAll();
    }

    /**
     * Tests {@link com.hazelcast.map.impl.operation.EntryOperation}.
     */
    @Test
    public void testEntryProcessorWhileTwoNodesAreBouncing_withoutPredicate() {
        testEntryProcessorWhileTwoNodesAreBouncing(false, false);
    }

    /**
     * Tests {@link com.hazelcast.map.impl.operation.MultipleEntryWithPredicateOperation}.
     */
    @Test
    public void testEntryProcessorWhileTwoNodesAreBouncing_withPredicateNoIndex() {
        testEntryProcessorWhileTwoNodesAreBouncing(true, false);
    }

    @Test
    public void testEntryProcessorWhileTwoNodesAreBouncing_withPredicateWithIndex() {
        testEntryProcessorWhileTwoNodesAreBouncing(true, true);
    }

    private void testEntryProcessorWhileTwoNodesAreBouncing(boolean withPredicate, boolean withIndex) {
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean isRunning = new AtomicBoolean(true);

        // start up three instances
        HazelcastInstance instance = newInstance(withIndex);
        HazelcastInstance instance2 = newInstance(withIndex);
        HazelcastInstance instance3 = newInstance(withIndex);

        assertClusterSize(3, instance, instance3);
        assertClusterSizeEventually(3, instance2);

        final IMap<Integer, ListHolder> map = instance.getMap(MAP_NAME);
        final ListHolder expected = new ListHolder();

        // initialize the list synchronously to ensure the map is correctly initialized
        InitMapProcessor initProcessor = new InitMapProcessor();
        for (int i = 0; i < ENTRIES; ++i) {
            map.executeOnKey(i, initProcessor);
        }
        assertEquals(ENTRIES, map.size());

        // spin up the thread that stops/starts the instance2 and instance3, always keeping one instance running
        Runnable runnable = new TwoNodesRestartingRunnable(startLatch, isRunning, withPredicate, instance2, instance3);
        Thread bounceThread = new Thread(runnable);
        bounceThread.start();

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
                if (withPredicate) {
                    EntryObject eo = new PredicateBuilder().getEntryObject();
                    Predicate keyPredicate = eo.key().equal(i);
                    map.executeOnEntries(processor, keyPredicate);
                } else {
                    map.executeOnKey(i, processor);
                }
            }
            // give processing time to catch up
            ++iteration;
        }

        // signal the bounce threads that we're done
        isRunning.set(false);

        // wait for the instance bounces to complete
        assertJoinable(bounceThread);

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

        for (int index = 0; index < ENTRIES; ++index) {
            ListHolder holder = map.get(index);
            assertEquals("The ListHolder should contain ITERATIONS entries", ITERATIONS, holder.size());
            for (int i = 0; i < ITERATIONS; i++) {
                assertEquals(i, holder.get(i));
            }
        }
    }

    private HazelcastInstance newInstance(boolean withIndex) {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(2);
        if (withIndex) {
            mapConfig.addMapIndexConfig(new MapIndexConfig("__key", true));
        }

        return instanceFactory.newHazelcastInstance(config);
    }

    private class TwoNodesRestartingRunnable implements Runnable {

        private final CountDownLatch start;
        private final AtomicBoolean isRunning;
        private final boolean withPredicate;

        private HazelcastInstance instance1;
        private HazelcastInstance instance2;

        private TwoNodesRestartingRunnable(CountDownLatch startLatch, AtomicBoolean isRunning, boolean withPredicate,
                                           HazelcastInstance h1, HazelcastInstance h2) {
            this.start = startLatch;
            this.isRunning = isRunning;
            this.withPredicate = withPredicate;
            this.instance1 = h1;
            this.instance2 = h2;
        }

        @Override
        public void run() {
            try {
                start.await();
                while (isRunning.get()) {
                    instance1.shutdown();
                    instance2.shutdown();
                    sleepMillis(10);
                    instance1 = newInstance(withPredicate);
                    instance2 = newInstance(withPredicate);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class InitMapProcessor extends AbstractEntryProcessor<Integer, ListHolder> {

        @Override
        public Object process(Map.Entry<Integer, ListHolder> entry) {
            entry.setValue(new ListHolder());
            return null;
        }
    }

    private static class IncrementProcessor extends AbstractEntryProcessor<Integer, ListHolder> {

        private final int nextVal;

        private IncrementProcessor(int nextVal) {
            this.nextVal = nextVal;
        }

        @Override
        public Object process(Map.Entry<Integer, ListHolder> entry) {
            ListHolder holder = entry.getValue();
            if (holder == null) {
                holder = new ListHolder();
            }

            holder.add(nextVal);
            entry.setValue(holder);
            return null;
        }
    }

    private static class ListHolder implements DataSerializable {

        private List<Integer> list = new ArrayList<Integer>();
        private int size;

        public ListHolder() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(list.size());
            for (Integer value : list) {
                out.writeInt(value);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            size = in.readInt();
            list = new ArrayList<Integer>(size);
            for (int i = 0; i < size; i++) {
                list.add(in.readInt());
            }
        }

        public void add(int value) {
            // EPs should be idempotent if consistency for such type of operations required
            if (!list.contains(value)) {
                list.add(value);
                size++;
            }
        }

        public int get(int index) {
            return list.get(index);
        }

        public int size() {
            return size;
        }
    }
}
