package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientReplicatedMapLiteMemberTest {

    private TestHazelcastFactory factory;

    private ClientConfig smartClientConfig;

    private ClientConfig dummyClientConfig;

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        smartClientConfig = new ClientConfig();
        dummyClientConfig = new ClientConfig();
        dummyClientConfig.getNetworkConfig().setSmartRouting(false);
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testReplicatedMapIsCreatedBySmartClient() {
        testReplicatedMapCreated(2, 1, smartClientConfig);
    }

    @Test
    public void testReplicatedMapIsCreatedByDummyClient() {
        testReplicatedMapCreated(2, 1, dummyClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnOnlyLiteMembersBySmartClient() {
        testReplicatedMapCreated(2, 0, smartClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnOnlyLiteMembersByDummyClient() {
        testReplicatedMapCreated(2, 0, dummyClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnSingleLiteMemberBySmartClient() {
        testReplicatedMapCreated(1, 0, smartClientConfig);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testReplicatedMapNotCreatedOnSingleLiteMemberByDummyClient() {
        testReplicatedMapCreated(1, 0, dummyClientConfig);
    }

    private void testReplicatedMapCreated(final int numberOfLiteNodes,
                                          final int numberOfDataNodes,
                                          final ClientConfig clientConfig) {
        createNodes(numberOfLiteNodes, numberOfDataNodes);

        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertNotNull(client.getReplicatedMap(randomMapName()));
    }

    @Test
    public void testReplicatedMapPutBySmartClient() {
        createNodes(3, 1);

        final HazelcastInstance client = factory.newHazelcastClient();
        final ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        assertNull(map.put(1, 2));
    }

    @Test
    public void testReplicatedMapPutByDummyClient() throws UnknownHostException {
        final List<HazelcastInstance> instances = createNodes(3, 1);
        configureDummyClientConnection(instances.get(0));

        final HazelcastInstance client = factory.newHazelcastClient(dummyClientConfig);

        final ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        map.put(1, 2);
    }

    private void configureDummyClientConnection(final HazelcastInstance instance) throws UnknownHostException {
        final InetSocketAddress socketAddress = getAddress(instance).getInetSocketAddress();
        dummyClientConfig.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        final ClientNetworkConfig networkConfig = dummyClientConfig.getNetworkConfig();
        networkConfig.addAddress(socketAddress.getHostName() + ":" + socketAddress.getPort());
    }

    private List<HazelcastInstance> createNodes(final int numberOfLiteNodes, final int numberOfDataNodes) {
        final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

        final Config liteConfig = new Config().setLiteMember(true);
        for (int i = 0; i < numberOfLiteNodes; i++) {
            instances.add(factory.newHazelcastInstance(liteConfig));
        }

        for (int i = 0; i < numberOfDataNodes; i++) {
            instances.add(factory.newHazelcastInstance());
        }

        final int clusterSize = numberOfLiteNodes + numberOfDataNodes;
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(clusterSize, instance);
        }

        return instances;
    }
}
