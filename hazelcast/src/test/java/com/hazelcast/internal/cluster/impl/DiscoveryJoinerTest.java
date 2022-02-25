/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiscoveryJoinerTest {

    @Mock
    private DiscoveryService service = mock(DiscoveryService.class);

    private List<DiscoveryNode> discoveryNodes;
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz;

    @Before
    public void init() throws Exception {
        discoveryNodes = new ArrayList<DiscoveryNode>(2);
        Address privateAddress = new Address("127.0.0.1", 5701);
        Address publicAddress = new Address("127.0.0.2", 5701);
        discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
        privateAddress = new Address("127.0.0.1", 5702);
        publicAddress = new Address("127.0.0.2", 5702);
        discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
        factory = new TestHazelcastInstanceFactory(1);
        hz = factory.newHazelcastInstance();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void test_DiscoveryJoiner_returns_public_address() {
        DiscoveryJoiner joiner = new DiscoveryJoiner(getNode(hz), service, true);
        doReturn(discoveryNodes).when(service).discoverNodes();
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[[127.0.0.2]:5701, [127.0.0.2]:5702]", addresses.toString());
    }

    @Test
    public void test_DiscoveryJoiner_returns_private_address_and_enrich_member_with_public_address() {
        DiscoveryJoiner joiner = new DiscoveryJoiner(getNode(hz), service, false);
        doReturn(discoveryNodes).when(service).discoverNodes();
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[[127.0.0.1]:5702]", addresses.toString());
    }

    @Test
    public void test_DiscoveryJoiner_enriches_member_with_public_address() {
        DiscoveryJoiner joiner = new DiscoveryJoiner(getNode(hz), service, false);
        doReturn(discoveryNodes).when(service).discoverNodes();
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[127.0.0.2]:5701", getNode(hz).getLocalMember().getAddressMap()
                .get(EndpointQualifier.resolve(ProtocolType.CLIENT, "public")).toString());
    }

    @Test
    public void test_DiscoveryJoiner_enriches_member_with_public_address_when_advanced_network_used()
            throws UnknownHostException {
        DiscoveryJoiner joiner = new DiscoveryJoiner(getNode(hz), service, false);
        doReturn(discoveryNodes).when(service).discoverNodes();
        getNode(hz).getLocalMember().getAddressMap().put(EndpointQualifier.CLIENT, new Address("127.0.0.1", 5703));
        Collection<Address> addresses = joiner.getPossibleAddresses();
        assertEquals("[127.0.0.2]:5703", getNode(hz).getLocalMember().getAddressMap()
                .get(EndpointQualifier.resolve(ProtocolType.CLIENT, "public")).toString());
    }

    @Test
    public void test_DiscoveryJoinerJoin_whenTargetMemberSet() {
        Node node = getNode(hz);
        node.config.getNetworkConfig().getJoin().getTcpIpConfig().setRequiredMember("127.0.0.1");
        DiscoveryJoiner joiner = new DiscoveryJoiner(node, service, true);
        doReturn(discoveryNodes).when(service).discoverNodes();

        joiner.join();
        assertTrue(node.getClusterService().isJoined());
    }

    @Test
    public void test_DiscoveryJoinerJoin_whenTargetMemberHasSameAddressAsNode() throws UnknownHostException {
        Node node = getNode(hz);
        String hostAddress = node.getThisAddress().getInetAddress().getHostAddress();
        node.config.getNetworkConfig().getJoin().getTcpIpConfig().setRequiredMember(hostAddress);

        List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(new SimpleDiscoveryNode(node.getThisAddress(), node.getThisAddress()));

        DiscoveryJoiner joiner = new DiscoveryJoiner(node, service, true);
        doReturn(nodes).when(service).discoverNodes();

        joiner.join();
        assertTrue(node.getClusterService().isJoined());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_DiscoveryJoinerConstructor_throws_whenTryCountInvalid() {
        Node node = spy(getNode(hz));
        HazelcastProperties properties = mock(HazelcastProperties.class);

        when(node.getProperties()).thenReturn(properties);
        when(properties.getInteger(ClusterProperty.TCP_JOIN_PORT_TRY_COUNT)).thenReturn(0);

        new DiscoveryJoiner(node, service, false);
    }
}
