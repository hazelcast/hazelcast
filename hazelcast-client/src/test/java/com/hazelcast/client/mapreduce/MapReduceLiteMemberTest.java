/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class MapReduceLiteMemberTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance client;

    private HazelcastInstance lite;

    private HazelcastInstance lite2;

    private HazelcastInstance instance;

    private HazelcastInstance instance2;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        lite = factory.newHazelcastInstance(new Config().setLiteMember(true));
        lite2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        instance = factory.newHazelcastInstance();
        instance2 = factory.newHazelcastInstance();

        assertClusterSize(4, lite, instance2);
        assertClusterSizeEventually(4, lite2, instance);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMapper()
            throws Exception {
        com.hazelcast.mapreduce.MapReduceLiteMemberTest.testMapper(client);
    }

    @Test(timeout = 60000)
    public void testKeyedMapperCollator()
            throws Exception {
        com.hazelcast.mapreduce.MapReduceLiteMemberTest.testKeyedMapperCollator(client);
    }

    @Test(timeout = 60000)
    public void testMapperComplexMapping()
            throws Exception {
        com.hazelcast.mapreduce.MapReduceLiteMemberTest.testMapperComplexMapping(client);
    }

    @Test(timeout = 60000)
    public void testMapperCollator()
            throws Exception {
        com.hazelcast.mapreduce.MapReduceLiteMemberTest.testMapperCollator(client);
    }

    @Test(timeout = 60000)
    public void testMapperReducerCollator()
            throws Exception {
        com.hazelcast.mapreduce.MapReduceLiteMemberTest.testMapperReducerCollator(client);
    }

    @Test(timeout = 120000)
    public void testMapReduceJobSubmissionWithNoDataNode() throws Exception {
        instance.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();
        assertClusterSizeEventually(2, lite, lite2);

        ICompletableFuture<Map<String, List<Integer>>> future = com.hazelcast.mapreduce.MapReduceLiteMemberTest
                .testMapReduceJobSubmissionWithNoDataNode(client);

        try {
            future.get(120, TimeUnit.SECONDS);
            fail("Map-reduce job should not be submitted when there is no data member");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

}
