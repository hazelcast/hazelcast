package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.client.impl.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ProxyManagerTest {

    private TestHazelcastFactory factory;

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testNextAddressToSendCreateRequestOnSingleDataMember() {
        final List<HazelcastInstance> instances = createNodes(3, 1);
        final Address dataInstanceAddress = getAddress(instances.get(3));

        final HazelcastInstance client = factory.newHazelcastClient();
        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        final ProxyManager proxyManager = clientInstanceImpl.getProxyManager();
        for (int i = 0; i < instances.size(); i++) {
            assertEquals(dataInstanceAddress, proxyManager.findNextAddressToSendCreateRequest());
        }
    }

    @Test
    public void testNextAddressToSendCreateRequestOnMultipleDataMembers() {
        final List<HazelcastInstance> instances = createNodes(3, 3);

        final HazelcastInstance client = factory.newHazelcastClient();
        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        Set<Address> addresses = new HashSet<Address>();
        final ProxyManager proxyManager = clientInstanceImpl.getProxyManager();
        for (int i = 0; i < instances.size() * 100; i++) {
            addresses.add(proxyManager.findNextAddressToSendCreateRequest());
        }

        assertEquals(3, addresses.size());
        for (HazelcastInstance lite : instances.subList(3, 6)) {
            assertTrue(addresses.contains(getAddress(lite)));
        }
    }

    @Test
    public void testNextAddressToSendCreateRequestOnMultipleLiteMembers() {
        final List<HazelcastInstance> instances = createNodes(3, 0);

        final HazelcastInstance client = factory.newHazelcastClient();
        final HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);

        Set<Address> addresses = new HashSet<Address>();
        final ProxyManager proxyManager = clientInstanceImpl.getProxyManager();
        for (int i = 0; i < instances.size() * 100; i++) {
            addresses.add(proxyManager.findNextAddressToSendCreateRequest());
        }

        assertEquals(1, addresses.size());
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
