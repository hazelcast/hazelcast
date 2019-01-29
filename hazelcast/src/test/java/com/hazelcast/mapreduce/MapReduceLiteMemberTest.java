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

package com.hazelcast.mapreduce;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.mapreduce.MapReduceTest.integerKvSource;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapReduceLiteMemberTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance instance;

    private HazelcastInstance instance2;

    private HazelcastInstance lite;

    private HazelcastInstance lite2;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(4);
        instance = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();
        lite = factory.newHazelcastInstance(new Config().setLiteMember(true));
        lite2 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertClusterSize(4, instance, lite2);
        assertClusterSizeEventually(4, instance2, lite);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMapper_fromLiteMember()
            throws Exception {
        testMapper(lite);
    }

    @Test(timeout = 60000)
    public void testMapper()
            throws Exception {
        testMapper(instance);
    }

    @Test(timeout = 60000)
    public void testKeyedMapperCollator_fromLiteMember()
            throws Exception {
        testKeyedMapperCollator(lite);
    }

    @Test(timeout = 60000)
    public void testKeyedMapperCollator()
            throws Exception {
        testKeyedMapperCollator(instance);
    }

    @Test(timeout = 60000)
    public void testMapperComplexMapping_fromLiteMember()
            throws Exception {
        testMapperComplexMapping(lite);
    }

    @Test(timeout = 60000)
    public void testMapperComplexMapping()
            throws Exception {
        testMapperComplexMapping(instance);
    }

    @Test(timeout = 60000)
    public void testMapperReducerChunked_fromLiteMember()
            throws Exception {
        testMapperReducerChunked(lite);
    }

    @Test(timeout = 60000)
    public void testMapperReducerChunked()
            throws Exception {
        testMapperReducerChunked(instance);
    }

    @Test(timeout = 60000)
    public void testMapperCollator_fromLiteMember()
            throws Exception {
        testMapperCollator(lite);
    }

    @Test(timeout = 60000)
    public void testMapperCollator()
            throws Exception {
        testMapperCollator(instance);
    }

    @Test(timeout = 60000)
    public void testMapperReducerCollator_fromLiteMember()
            throws Exception {
        testMapperReducerCollator(lite);
    }

    @Test(timeout = 60000)
    public void testMapperReducerCollator()
            throws Exception {
        testMapperReducerCollator(instance);
    }

    @Test(expected = IllegalStateException.class, timeout = 60000)
    public void testMapReduceJobSubmissionWithNoDataNode() {
        instance.shutdown();
        instance2.shutdown();
        assertClusterSizeEventually(2, lite, lite2);

        testMapReduceJobSubmissionWithNoDataNode(lite);
    }

    public static void testMapper(final HazelcastInstance instance)
            throws Exception {
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, randomMapName(), 100);
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new MapReduceTest.TestMapper()).submit();
        Map<String, List<Integer>> result = future.get();

        assertEquals(100, result.size());
        for (List<Integer> value : result.values()) {
            assertEquals(1, value.size());
        }
    }

    public static void testKeyedMapperCollator(final HazelcastInstance instance)
            throws Exception {
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, randomMapName(), 10000);
        ICompletableFuture<Integer> future = job.onKeys(50).mapper(new MapReduceTest.TestMapper())
                .submit(new MapReduceTest.GroupingTestCollator());

        int result = future.get();

        assertEquals(50, result);
    }

    public static void testMapperComplexMapping(final HazelcastInstance instance)
            throws Exception {
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, randomMapName(), 100);
        ICompletableFuture<Map<String, List<Integer>>> future = job.mapper(new MapReduceTest.GroupingTestMapper(2)).submit();

        Map<String, List<Integer>> result = future.get();

        assertEquals(1, result.size());
        assertEquals(25, result.values().iterator().next().size());
    }

    public static void testMapperReducerChunked(final HazelcastInstance instance)
            throws Exception {
        final String mapName = randomMapName();
        JobTracker tracker = instance.getJobTracker(mapName);
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, mapName, 10000);
        JobCompletableFuture<Map<String, Integer>> future = job.chunkSize(10).mapper(new MapReduceTest.GroupingTestMapper())
                .reducer(new MapReduceTest.TestReducerFactory()).submit();

        final TrackableJob trackableJob = tracker.getTrackableJob(future.getJobId());
        final JobProcessInformation processInformation = trackableJob.getJobProcessInformation();
        Map<String, Integer> result = future.get();

        int[] expectedResults = new int[4];
        for (int i = 0; i < 10000; i++) {
            int index = i % 4;
            expectedResults[index] += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResults[i], (int) result.get(String.valueOf(i)));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                if (processInformation.getProcessedRecords() < 10000) {
                    System.err.println(processInformation.getProcessedRecords());
                }
                assertEquals(10000, processInformation.getProcessedRecords());
            }
        });
    }

    public static void testMapperCollator(final HazelcastInstance instance)
            throws Exception {
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, randomMapName(), 100);
        ICompletableFuture<Integer> future = job.mapper(new MapReduceTest.GroupingTestMapper())
                .submit(new MapReduceTest.GroupingTestCollator());

        int result = future.get();

        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    public static void testMapperReducerCollator(final HazelcastInstance instance)
            throws Exception {
        Job<Integer, Integer> job = populateMapAndCreateJob(instance, randomMapName(), 100);
        ICompletableFuture<Integer> future = job.mapper(new MapReduceTest.GroupingTestMapper())
                .reducer(new MapReduceTest.TestReducerFactory())
                .submit(new MapReduceTest.TestCollator());

        int result = future.get();

        int expectedResult = 0;
        for (int i = 0; i < 100; i++) {
            expectedResult += i;
        }

        for (int i = 0; i < 4; i++) {
            assertEquals(expectedResult, result);
        }
    }

    public static ICompletableFuture<Map<String, List<Integer>>> testMapReduceJobSubmissionWithNoDataNode(
            final HazelcastInstance instance) {
        final String mapName = randomMapName();
        JobTracker tracker = instance.getJobTracker(mapName);
        final IMap<Integer, Integer> m1 = instance.getMap(mapName);
        Job<Integer, Integer> job = tracker.newJob(integerKvSource(m1));
        return job.mapper(new MapReduceTest.TestMapper()).submit();
    }

    public static Job<Integer, Integer> populateMapAndCreateJob(final HazelcastInstance instance, final String mapName,
                                                                final int count) {
        final IMap<Integer, Integer> m1 = instance.getMap(mapName);
        for (int i = 0; i < count; i++) {
            m1.put(i, i);
        }

        JobTracker tracker = instance.getJobTracker(mapName);
        return tracker.newJob(integerKvSource(m1));
    }

}
