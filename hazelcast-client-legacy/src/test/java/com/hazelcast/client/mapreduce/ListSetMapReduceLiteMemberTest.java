package com.hazelcast.client.mapreduce;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListSetMapReduceLiteMemberTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance client;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        final HazelcastInstance lite = factory.newHazelcastInstance(new Config().setLiteMember(false));
        final HazelcastInstance lite2 = factory.newHazelcastInstance(new Config().setLiteMember(false));
        final HazelcastInstance instance1 = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();

        assertClusterSizeEventually(4, lite);
        assertClusterSizeEventually(4, lite2);
        assertClusterSizeEventually(4, instance1);
        assertClusterSizeEventually(4, instance2);

        client = factory.newHazelcastClient();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(timeout = 60000)
    public void testMapReduceWithList()
            throws Exception {
        com.hazelcast.mapreduce.ListSetMapReduceLiteMemberTest.testMapReduceWithList(client);
    }

    @Test(timeout = 60000)
    public void testMapReduceWithSet()
            throws Exception {
        com.hazelcast.mapreduce.ListSetMapReduceLiteMemberTest.testMapReduceWithSet(client);
    }

}
