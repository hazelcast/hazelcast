package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ProxyEqualityTest {

    private static final String atomicName = "foo";

    private static final String groupAName = "GroupA";
    private static final String groupBName = "GroupB";


    static HazelcastInstance client1GroupA;
    static HazelcastInstance client2GroupA;
    static HazelcastInstance server1GroupA;

    static HazelcastInstance client1GroupB;
    static HazelcastInstance server1GroupB;


    @BeforeClass
    public static void setup() throws Exception {
        Config config = new Config();
        config.getGroupConfig().setName(groupAName);
        server1GroupA = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(config.getGroupConfig().getName()));
        client1GroupA = HazelcastClient.newHazelcastClient(clientConfig);
        client2GroupA = HazelcastClient.newHazelcastClient(clientConfig);

        //setup Group B
        config = new Config();
        config.getGroupConfig().setName(groupBName);
        server1GroupB = Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(config.getGroupConfig().getName()));
        client1GroupB = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTwoClientProxiesFromTheSameInstanceAreEquals() {

        ClientProxy ref1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy ref2 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);

        assertEquals(ref1, ref2);
    }

    @Test
    public void testProxiesAreCached() {

        ClientProxy ref1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy ref2 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);

        assertSame(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromDifferentInstancesAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy ref2 = (ClientProxy) client1GroupB.getAtomicLong(atomicName);

        assertNotEquals(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromTwoDifferentClientsConnectedToTheSameInstanceAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy ref2 = (ClientProxy) client2GroupA.getAtomicLong(atomicName);

        assertNotEquals(ref1, ref2);
    }
}
