package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProxyEqualityTest {

    private Map<String, HazelcastInstance> testInstancesCache;


    @After
    @Before
    public void cleanup() throws Exception {
        testInstancesCache = new HashMap<String, HazelcastInstance>();
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTwoClientProxiesFromTheSameInstanceAreEquals() {
        HazelcastInstance h1Client = createClient("group1");

        ClientProxy ref1 = (ClientProxy) h1Client.getAtomicLong("foo");
        ClientProxy ref2 = (ClientProxy) h1Client.getAtomicLong("foo");

        assertEquals(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromDifferentInstancesAreNotEquals() {
        HazelcastInstance h1Client = createClient("group1");
        HazelcastInstance h2Client = createClient("group2");

        ClientProxy ref1 = (ClientProxy) h1Client.getAtomicLong("foo");
        ClientProxy ref2 = (ClientProxy) h2Client.getAtomicLong("foo");

        assertNotEquals(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromTwoDifferentClientsConnectedToTheSameInstanceAreNotEquals() {

        HazelcastInstance h1Client = createClient("group1");
        HazelcastInstance h2Client = createClient("group1");

        ClientProxy ref1 = (ClientProxy) h1Client.getAtomicLong("foo");
        ClientProxy ref2 = (ClientProxy) h2Client.getAtomicLong("foo");

        assertNotEquals(ref1, ref2);

    }

    private HazelcastInstance createClient(String serverInstanceGroupName) {

        HazelcastInstance serverInstance = testInstancesCache.get(serverInstanceGroupName);

        if (serverInstance == null) {
            Config config = new Config();
            config.getGroupConfig().setName(serverInstanceGroupName);
            serverInstance = Hazelcast.newHazelcastInstance(config);
            testInstancesCache.put(serverInstanceGroupName, serverInstance);
        }

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(serverInstance.getConfig().getGroupConfig().getName(),
                serverInstance.getConfig().getGroupConfig().getPassword()));

        return HazelcastClient.newHazelcastClient(clientConfig);

    }


}
