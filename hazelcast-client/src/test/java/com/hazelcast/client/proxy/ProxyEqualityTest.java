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

    private static final String nameGroupA = "GroupA";
    private static final String nameGroupB = "GroupB";

    private static HazelcastInstance client1GroupA;
    private static HazelcastInstance client2GroupA;

    private static HazelcastInstance client1GroupB;

    @BeforeClass
    public static void beforeClass() {
        // Setup group A
        Config config = new Config();
        config.getGroupConfig().setName(nameGroupA);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(config.getGroupConfig().getName()));
        client1GroupA = HazelcastClient.newHazelcastClient(clientConfig);
        client2GroupA = HazelcastClient.newHazelcastClient(clientConfig);

        // Setup group B
        config = new Config();
        config.getGroupConfig().setName(nameGroupB);
        Hazelcast.newHazelcastInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(config.getGroupConfig().getName()));
        client1GroupB = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTwoClientProxiesFromTheSameInstanceAreEquals() {
        ClientProxy equalsProxy1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy equalsProxy2 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);

        assertEquals(equalsProxy1, equalsProxy2);
    }

    @Test
    public void testProxiesAreCached() {
        ClientProxy sameProxy1 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy sameProxy2 = (ClientProxy) client1GroupA.getAtomicLong(atomicName);

        assertSame(sameProxy1, sameProxy2);
    }

    @Test
    public void testTwoClientProxiesFromTwoDifferentClientsConnectedToTheSameInstanceAreNotEquals() {
        ClientProxy proxy1FromSameGroup = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy proxy2FromSameGroup = (ClientProxy) client2GroupA.getAtomicLong(atomicName);

        assertNotEquals(proxy1FromSameGroup, proxy2FromSameGroup);
    }

    @Test
    public void testTwoClientProxiesFromDifferentInstancesAreNotEquals() {
        ClientProxy proxyFromGroupA = (ClientProxy) client1GroupA.getAtomicLong(atomicName);
        ClientProxy proxyFromGroupB = (ClientProxy) client1GroupB.getAtomicLong(atomicName);

        assertNotEquals(proxyFromGroupA, proxyFromGroupB);
    }
}
