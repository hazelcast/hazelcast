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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LocalAddressRegistryIntegrationTest extends HazelcastTestSupport {

    // MEMBER & WAN & CLIENT addresses of the connection initiator
    private static final Address INITIATOR_MEMBER_ADDRESS;
    private static final Address INITIATOR_WAN_ADDRESS;
    private static final Address INITIATOR_CLIENT_ADDRESS;

    // server-side member addresses
    private static final Address SERVER_MEMBER_ADDRESS;
    private static final Address SERVER_CLIENT_ADDRESS;
    private static final Address SERVER_WAN_ADDRESS;

    static {
        try {
            INITIATOR_MEMBER_ADDRESS = new Address("127.0.0.1", 5702);
            INITIATOR_CLIENT_ADDRESS = new Address("127.0.0.1", 6001);
            INITIATOR_WAN_ADDRESS = new Address("127.0.0.1", 9000);

            SERVER_MEMBER_ADDRESS = new Address("127.0.0.1", 5701);
            SERVER_CLIENT_ADDRESS = new Address("127.0.0.1", 6000);
            SERVER_WAN_ADDRESS = new Address("127.0.0.1", 10000);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenSomeConnectionClosedBetweenMembers_registeredAddresses_should_not_be_cleaned_up() {
        int timeoutSecs = 10;
        int tcpChannelsPerConnection = 3;
        System.setProperty("tcp.channels.per.connection", String.valueOf(tcpChannelsPerConnection));

        // create members
        HazelcastInstance serverMember = Hazelcast.newHazelcastInstance(createConfigForServer());
        HazelcastInstance initiatorMember = Hazelcast.newHazelcastInstance(createConfigForInitiator());
        UUID initiatorMemberUuid = initiatorMember.getCluster().getLocalMember().getUuid();
        assertClusterSize(2, serverMember, initiatorMember);

        createConnectionsOnEachPlane(serverMember);
        Node serverNode = getNode(serverMember);
        TcpServerConnectionManager connectionManager = (TcpServerConnectionManager) serverNode.getServer()
                .getConnectionManager(EndpointQualifier.MEMBER);
        assertTrueEventually(() ->
                assertGreaterOrEquals(
                        "The number of connections must be greater than the number of channels.",
                        connectionManager.getConnections().size(),
                        tcpChannelsPerConnection
                ), timeoutSecs);

        LinkedAddresses registeredAddressesOfInitiatorMember = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(initiatorMemberUuid);
        assertNotNull(registeredAddressesOfInitiatorMember);
        assertContains(registeredAddressesOfInitiatorMember.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);
        int previousNoConnections = connectionManager.getConnections().size();
        closeRandomConnection(new ArrayList<>(connectionManager.getConnections()));
        assertTrueEventually(() ->
                assertEquals(
                        previousNoConnections - 1,
                        connectionManager.getConnections().size()
                ), timeoutSecs);

        LinkedAddresses registeredAddressesAfterConnectionClose = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(initiatorMemberUuid);
        assertNotNull(registeredAddressesAfterConnectionClose);
        assertContains(registeredAddressesAfterConnectionClose.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);

        waitAllForSafeState(serverMember, initiatorMember);
    }

    @Test
    public void whenAllConnectionClosedBetweenMembers_registeredAddresses_should_be_cleaned_up() {
        int timeoutSecs = 10;
        int tcpChannelsPerConnection = 3;
        System.setProperty("tcp.channels.per.connection", String.valueOf(tcpChannelsPerConnection));

        // create members
        HazelcastInstance serverMember = Hazelcast.newHazelcastInstance(createConfigForServer());
        HazelcastInstance initiatorMember = Hazelcast.newHazelcastInstance(createConfigForInitiator());
        UUID iniatiatorMemberUuid = initiatorMember.getCluster().getLocalMember().getUuid();
        assertClusterSize(2, serverMember, initiatorMember);
        createConnectionsOnEachPlane(serverMember);

        Node serverNode = getNode(serverMember);
        TcpServerConnectionManager connectionManager = (TcpServerConnectionManager) serverNode.getServer()
                .getConnectionManager(EndpointQualifier.MEMBER);
        assertTrueEventually(() ->
                assertGreaterOrEquals(
                        "The number of connections must be greater than the number of channels.",
                        connectionManager.getConnections().size(),
                        tcpChannelsPerConnection
                ), timeoutSecs);

        LinkedAddresses registeredAddressesOfInitiatorMember = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(iniatiatorMemberUuid);

        assertNotNull(registeredAddressesOfInitiatorMember);
        assertContains(registeredAddressesOfInitiatorMember.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);
        initiatorMember.shutdown();
        assertTrueEventually(() ->
                assertEquals(
                        0,
                        connectionManager.getConnections().size()
                ), timeoutSecs);
        LinkedAddresses registeredAddressesAfterAllConnectionsClosed = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(iniatiatorMemberUuid);
        assertNull(registeredAddressesAfterAllConnectionsClosed);

    }


    private Config createConfigForServer() {
        Config config = smallInstanceConfig();
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getTcpIpConfig().setEnabled(true);
        ServerSocketEndpointConfig memberServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_MEMBER_ADDRESS.getPort());
        memberServerSocketConfig.getInterfaces().addInterface(SERVER_MEMBER_ADDRESS.getHost());
        ServerSocketEndpointConfig clientServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_CLIENT_ADDRESS.getPort());
        clientServerSocketConfig.getInterfaces().addInterface(SERVER_CLIENT_ADDRESS.getHost());
        ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig()
                .setName("wan")
                .setPort(SERVER_WAN_ADDRESS.getPort());
        wanServerSocketConfig.getInterfaces().addInterface(SERVER_WAN_ADDRESS.getHost());

        memberServerSocketConfig.getInterfaces().addInterface("127.0.0.1");
        advancedNetworkConfig.setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);
        return config;
    }

    private Config createConfigForInitiator() {
        Config config = smallInstanceConfig();
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getTcpIpConfig().setEnabled(true).addMember(
                SERVER_MEMBER_ADDRESS.getHost() + ":" + SERVER_MEMBER_ADDRESS.getPort()
        );

        ServerSocketEndpointConfig memberServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(INITIATOR_MEMBER_ADDRESS.getPort());
        memberServerSocketConfig.getInterfaces().addInterface(INITIATOR_MEMBER_ADDRESS.getHost());
        ServerSocketEndpointConfig clientServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(INITIATOR_CLIENT_ADDRESS.getPort());
        ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig()
                .setName("wan")
                .setPort(SERVER_WAN_ADDRESS.getPort());
        wanServerSocketConfig.getInterfaces().addInterface(INITIATOR_WAN_ADDRESS.getHost());

        memberServerSocketConfig.getInterfaces().addInterface("127.0.0.1");
        advancedNetworkConfig.setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);

        return config;
    }

    private void createConnectionsOnEachPlane(HazelcastInstance hz) {
        IMap<String, String> dummy = hz.getMap(randomMapName());
        hz.getPartitionService().getPartitions().forEach(
                partition -> {
                    String key = randomKeyNameOwnedByPartition(hz, partition.getPartitionId());
                    dummy.put(key, key);
                }
        );
        dummy.destroy();
    }

    private void closeRandomConnection(List<Connection> connections) {
        Random random = new Random();
        int randomIdx = random.nextInt(connections.size());
        connections.get(randomIdx).close("Failure is injected", null);
    }

    private String randomKeyNameOwnedByPartition(HazelcastInstance hz, int partitionId) {
        PartitionService partitionService = hz.getPartitionService();
        while (true) {
            String name = randomString();
            Partition partition = partitionService.getPartition(name);
            if (partition.getPartitionId() == partitionId) {
                return name;
            }
        }
    }
}
