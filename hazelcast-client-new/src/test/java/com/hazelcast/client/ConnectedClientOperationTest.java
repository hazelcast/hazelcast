package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ConnectedClientOperationTest extends HazelcastTestSupport {

    @BeforeClass
    @AfterClass
    public static void cleanupClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNumberOfConnectedClients() throws Exception{

        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastClient.newHazelcastClient(clientConfig);

        Node node1 = TestUtil.getNode(h1);
        Map<ClientType, Integer> clientStats = node1.getClusterService().getConnectedClientStats();

        assertEquals(6, clientStats.get(ClientType.JAVA).intValue());
        assertEquals(0, clientStats.get(ClientType.CPP).intValue());
        assertEquals(0, clientStats.get(ClientType.CSHARP).intValue());

    }
}
