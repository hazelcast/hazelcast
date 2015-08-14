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

package com.hazelcast.client.mapreduce;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.ListSetMapReduceTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientListSetMapReduceTest
        extends AbstractClientMapReduceJobTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMapReduceWithList() throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = hazelcastFactory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        int expectedResult = 0;
        IList<Integer> list = h1.getList("default");
        for (int o = 0; o < 100; o++) {
            list.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = client.getJobTracker("default");
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromList(list));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture = job.chunkSize(10).mapper(new ListSetMapReduceTest.ListSetMapper())
                                                                         .combiner(new ListSetMapReduceTest.ListSetCombinerFactory())
                                                                         .reducer(new ListSetMapReduceTest.ListSetReducerFactory()).submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1, result.size());

        int count = 0;
        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(list.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

    @Test(timeout = 60000)
    public void testMapReduceWithSet() throws Exception {
        Config config = buildConfig();

        HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance h3 = hazelcastFactory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        int expectedResult = 0;
        ISet<Integer> set = h1.getSet("default");
        for (int o = 0; o < 100; o++) {
            set.add(o);
            expectedResult += o;
        }

        JobTracker jobTracker = client.getJobTracker("default");
        Job<String, Integer> job = jobTracker.newJob(KeyValueSource.fromSet(set));
        ICompletableFuture<Map<String, Integer>> ICompletableFuture = job.chunkSize(10).mapper(new ListSetMapReduceTest.ListSetMapper())
                                                                         .combiner(new ListSetMapReduceTest.ListSetCombinerFactory())
                                                                         .reducer(new ListSetMapReduceTest.ListSetReducerFactory()).submit();

        Map<String, Integer> result = ICompletableFuture.get();

        assertEquals(1, result.size());

        for (Map.Entry<String, Integer> entry : result.entrySet()) {
            assertEquals(set.getName(), entry.getKey());
            assertEquals(expectedResult, (int) entry.getValue());
        }
    }

}
