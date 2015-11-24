package com.hazelcast.client.mapreduce.aggregation;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapAggregationLiteMemberTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance client;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        final HazelcastInstance lite = factory.newHazelcastInstance(new Config().setLiteMember(true));
        final HazelcastInstance instance1 = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();

        assertClusterSizeEventually(3, lite);
        assertClusterSizeEventually(3, instance1);
        assertClusterSizeEventually(3, instance2);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMaxAggregation() {
        com.hazelcast.mapreduce.aggregation.MapAggregationLiteMemberTest.testMaxAggregation(client);
    }

}
