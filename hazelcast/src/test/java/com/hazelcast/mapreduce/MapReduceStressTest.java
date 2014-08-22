/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.MapReduceTest.GroupingTestMapper;
import com.hazelcast.mapreduce.MapReduceTest.ObjectCombinerFactory;
import com.hazelcast.mapreduce.MapReduceTest.ObjectReducerFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
@Repeat(100)
public class MapReduceStressTest
        extends HazelcastTestSupport {

    private static final int MAP_ELEMENTS = 100;
    private static final int PARALLEL_TASKS = 50;
    private static final int HAZELCAST_INSTANCE_COUNT = 3;
    private static final String MAP_NAME = "fooo";

    @Test(timeout = 60000)
    public void test()
            throws Exception {

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(HAZELCAST_INSTANCE_COUNT);

        HazelcastInstance[] instances = new HazelcastInstance[HAZELCAST_INSTANCE_COUNT];
        for (int i = 0; i < HAZELCAST_INSTANCE_COUNT; i++) {
            instances[i] = hazelcastInstanceFactory.newHazelcastInstance();
        }

        for (int i = 0; i < HAZELCAST_INSTANCE_COUNT; i++) {
            assertClusterSizeEventually(HAZELCAST_INSTANCE_COUNT, instances[i]);
        }

        IMap<Integer, Integer> m1 = instances[0].getMap(MAP_NAME);
        for (int i = 0; i < MAP_ELEMENTS; i++) {
            m1.put(i, i);
        }

        AtomicReferenceArray<Object> resultHolder = new AtomicReferenceArray<Object>(PARALLEL_TASKS);
        CountDownLatch latch = new CountDownLatch(PARALLEL_TASKS);

        int[] expectedResults = new int[4];
        for (int i = 0; i < MAP_ELEMENTS; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        Random random = new Random();
        for (int o = 0; o < PARALLEL_TASKS; o++) {
            HazelcastInstance hazelcastInstance = instances[random.nextInt(instances.length)];
            IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
            Runnable task = new MapReduceOperation(hazelcastInstance, map, o, resultHolder, latch);
            Thread thread = new Thread(task);
            thread.start();
        }

        latch.await();

        for (int o = 0; o < PARALLEL_TASKS; o++) {
            Object result = resultHolder.get(o);
            if (result instanceof Throwable) {
                ((Throwable) result).printStackTrace();
                fail();
            }
            Map<Integer, Integer> map = (Map<Integer, Integer>) result;
            for (int i = 0; i < 4; i++) {
                assertEquals(BigInteger.valueOf(expectedResults[i]), map.get(String.valueOf(i)));
            }
        }
    }

    private static class MapReduceOperation
            implements Runnable {

        private final HazelcastInstance hazelcastInstance;
        private final IMap<Integer, Integer> map;
        private final int taskId;
        private final AtomicReferenceArray<Object> resultHolder;
        private final CountDownLatch latch;

        private MapReduceOperation(HazelcastInstance hazelcastInstance, IMap<Integer, Integer> map, int taskId,
                                   AtomicReferenceArray<Object> resultHolder, CountDownLatch latch) {
            this.hazelcastInstance = hazelcastInstance;
            this.map = map;
            this.taskId = taskId;
            this.resultHolder = resultHolder;
            this.latch = latch;
        }

        @Override
        public void run() {
            JobTracker jobTracker = hazelcastInstance.getJobTracker(HazelcastTestSupport.randomString());
            Job<Integer, Integer> job = jobTracker.newJob(KeyValueSource.fromMap(map));
            JobCompletableFuture<Map<String, BigInteger>> future = job.chunkSize(0).mapper(new GroupingTestMapper())
                                                                      .combiner(new ObjectCombinerFactory())
                                                                      .reducer(new ObjectReducerFactory()).submit();

            try {
                Map<String, BigInteger> result = future.get();
                resultHolder.set(taskId, result);
            } catch (Throwable t) {
                resultHolder.set(taskId, t);
            } finally {
                latch.countDown();
            }

        }
    }
}
