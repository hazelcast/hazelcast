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
import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;
import com.hazelcast.mapreduce.ListSetMapReduceTest.ListSetCombinerFactory;
import com.hazelcast.mapreduce.ListSetMapReduceTest.ListSetMapper;
import com.hazelcast.mapreduce.ListSetMapReduceTest.ListSetReducerFactory;
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

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListSetMapReduceLiteMemberTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance instance;

    private HazelcastInstance lite;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(4);
        instance = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();
        final Config liteConfig = new Config().setLiteMember(false);
        lite = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance lite2 = factory.newHazelcastInstance(liteConfig);

        assertClusterSize(4, instance, lite2);
        assertClusterSizeEventually(4, instance2, lite);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 600000)
    public void testMapReduceWithList_fromLite()
            throws Exception {
        testMapReduceWithList(lite);
    }

    @Test(timeout = 60000)
    public void testMapReduceWithList()
            throws Exception {
        testMapReduceWithList(instance);
    }

    @Test(timeout = 60000)
    public void testMapReduceWithSet_fromLiteMember()
            throws Exception {
        testMapReduceWithSet(lite);
    }

    @Test(timeout = 60000)
    public void testMapReduceWithSet()
            throws Exception {
        testMapReduceWithSet(instance);
    }

    public static void testMapReduceWithList(final HazelcastInstance instance)
            throws Exception {
        int expectedResult = 0;
        final String listName = randomName();
        IList<Integer> list = instance.getList(listName);
        for (int o = 0; o < 100; o++) {
            list.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = instance.getJobTracker(listName);
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromList(list));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture = job.chunkSize(10).mapper(new ListSetMapper())
                .combiner(new ListSetCombinerFactory())
                .reducer(new ListSetReducerFactory()).submit();

        Map<String, Integer> result = ICompletableFuture.get();
        assertEquals(1, result.size());

        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(list.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

    public static void testMapReduceWithSet(final HazelcastInstance instance)
            throws Exception {
        int expectedResult = 0;
        final String setName = randomName();
        ISet<Integer> set = instance.getSet(setName);
        for (int o = 0; o < 100; o++) {
            set.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = instance.getJobTracker(setName);
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromSet(set));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture = job.chunkSize(10).mapper(new ListSetMapper())
                .combiner(new ListSetCombinerFactory())
                .reducer(new ListSetReducerFactory()).submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1, result.size());

        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(set.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

}
