package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiscoveryJoinerTest {

    @Mock
    private DiscoveryService service = mock(DiscoveryService.class);

    List<DiscoveryNode> discoveryNodes;
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz;

    @Before
    public void init() throws Exception {
        discoveryNodes = new ArrayList<DiscoveryNode>(2);
        Address publicAddress = new Address("127.0.0.1", 50001);
        Address privateAddress = new Address("127.0.0.2", 50001);
        discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
        publicAddress = new Address("127.0.0.1", 50002);
        privateAddress = new Address("127.0.0.2", 50002);
        discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
        factory = new TestHazelcastInstanceFactory(1);
        hz = factory.newHazelcastInstance();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void test_DiscoveryJoiner_returns_public_address() throws Exception {
        DiscoveryJoiner joiner = new DiscoveryJoiner(TestUtil.getNode(hz), service, true);
        doReturn(discoveryNodes).when(service).discoverNodes();
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[[127.0.0.1]:50001, [127.0.0.1]:50002]", addresses.toString());
    }

    @Test
    public void test_DiscoveryJoiner_returns_private_address() throws Exception {
        DiscoveryJoiner joiner = new DiscoveryJoiner(TestUtil.getNode(hz), service, false);
        doReturn(discoveryNodes).when(service).discoverNodes();
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[[127.0.0.2]:50001, [127.0.0.2]:50002]", addresses.toString());
    }
}
