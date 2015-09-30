package com.hazelcast.client.partitionservice;

import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.ClientTestUtil.getHazelcastClientInstanceImpl;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPartitionServiceLiteMemberTest
        extends TestHazelcastFactory {

    private TestHazelcastFactory factory;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void testGetPartitionsBlockingFailWithOnlyLiteMember() {
        factory.newHazelcastInstance(new Config().setLiteMember(true));

        final HazelcastInstance client = factory.newHazelcastClient();
        final ClientPartitionService clientPartitionService = getClientPartitionService(client);
        clientPartitionService.getPartitionOwner(0);
    }

    @Test
    public void testPartitionsBlockingSucceedsWithLiteMemberAndDataMember() {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance(new Config().setLiteMember(true));

        final HazelcastInstance client = factory.newHazelcastClient();
        final ClientPartitionService clientPartitionService = getClientPartitionService(client);
        assertNotNull(clientPartitionService.getPartitionOwner(0));
    }

    private ClientPartitionService getClientPartitionService(HazelcastInstance client) {
        return getHazelcastClientInstanceImpl(client).getClientPartitionService();
    }

}
