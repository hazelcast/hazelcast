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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

        assertClusterSizeEventually(4, lite);
        assertClusterSizeEventually(4, lite2);
        assertClusterSizeEventually(4, instance);
        assertClusterSizeEventually(4, instance2);

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
        assertClusterSizeEventually(2, lite);
        assertClusterSizeEventually(2, lite2);

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
