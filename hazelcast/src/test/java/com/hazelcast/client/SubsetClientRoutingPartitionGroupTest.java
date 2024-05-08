/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.client.impl.clientside.SubsetMembers;
import com.hazelcast.client.impl.clientside.SubsetMembersImpl;
import com.hazelcast.client.impl.clientside.SubsetMembersView;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A test that verifies that a subset client, can send request
 * to a wrong node, but still can get responses to its requests.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubsetClientRoutingPartitionGroupTest extends ClientTestSupport {

    @Rule
    public OverridePropertyRule setProp = OverridePropertyRule.set(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, "true");

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void after_scaling_up_grown_member_group_includes_first_member_group() {
        // 1. Start server 1
        Config configA = newConfigWithAttribute("A");
        HazelcastInstance server1 = hazelcastFactory.newHazelcastInstance(configA);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false).getSubsetRoutingConfig().setEnabled(true)
                    .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap clientMap = client.getMap("map");
        for (int i = 0; i < 1_000; i++) {
            clientMap.set(i, i);
        }

        SubsetMembers subsetMembers = getHazelcastClientInstanceImpl(client)
                .getClientClusterService().getSubsetMembers();

        // 2. Get members of subset
        Set<UUID> members1 = subsetMembers.getSubsetMembersView().members();

        // 3. Start server 2 and ensure members in subset doesn't change
        HazelcastInstance server2 = hazelcastFactory.newHazelcastInstance(configA);
        assertTrueEventually(() -> {
            Set<UUID> members = subsetMembers.getSubsetMembersView().members();
            assertEquals(1, members.size());
            assertTrue(members.containsAll(members1));
        });

        // 4. Start server 3 and ensure member group is
        // growing by including first group's members Here
        // size of subset can be one of 1 or 2 but we need
        // to be sure that subset still has initial member.
        HazelcastInstance server3 = hazelcastFactory.newHazelcastInstance(configA);
        assertTrueEventually(() -> {
            Set<UUID> members = subsetMembers.getSubsetMembersView().members();
            assertFalse(members.isEmpty());
            assertTrue(members.containsAll(members1));
        });

        // 5. Start server 4 and ensure member group is growing
        // by including first group's members Now we have 4 serves
        // and they are grouped as 2-member groups so we can
        // expect our subset should also grow to 2. And after this
        // growing, the subset still includes 1st member in it.
        HazelcastInstance server4 = hazelcastFactory.newHazelcastInstance(configA);
        assertTrueEventually(() -> {
            Set<UUID> members = subsetMembers.getSubsetMembersView().members();
            assertEquals(2, members.size());
            assertTrue(members.containsAll(members1));
        });
    }

    @Test
    public void terminated_members_are_removed_from_memberGroupView() {
        // 1. create member group A
        Config configA = newConfigWithAttribute("A");

        List<HazelcastInstance> groupA = initNodes(3, configA);

        assertClusterSizeEventually(3, groupA);
        IMap<Object, Object> map = groupA.get(0).getMap("a");
        int entryCount = 10_000;
        for (int i = 0; i < entryCount; i++) {
            map.set(i, i);
        }
        assertAllInSafeState(groupA);

        // 2. Connect client
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false).getSubsetRoutingConfig().setEnabled(true)
                    .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // 3. Populate data
        IMap<Object, Object> clientMap = client.getMap("a");
        for (int i = 0; i < entryCount; i++) {
            clientMap.set(i, i);
        }

        // 4. Make sure subset client connects to subset of
        // members, number of connected subset servers can be 1
        // or 2 depending on the authenticator member's group.
        final int connectedServerCount = ((SubsetMembersImpl) getHazelcastClientInstanceImpl(client)
                .getClientClusterService().getSubsetMembers()).memberCountInSubset();
        makeSureConnectedToServers(client, connectedServerCount);

        // 5. Create member group B
        Config configB = newConfigWithAttribute("B");
        List<HazelcastInstance> groupB = initNodes(4, configB);

        // 6. Read data
        for (int i = 0; i < entryCount; i++) {
            map.get(i);
        }

        // 7. Get client's member group view
        SubsetMembers subsetMembers
                = getHazelcastClientInstanceImpl(client).getClientClusterService().getSubsetMembers();
        SubsetMembersView subsetMembersView = subsetMembers.getSubsetMembersView();
        Collection<UUID> subsetMemberUuidsBeforeTerminationOfGroupA = subsetMembersView.members();

        // 8. Terminate all servers in group A
        for (HazelcastInstance instance : groupA) {
            instance.getLifecycleService().terminate();
        }

        // 9. Read again
        for (int i = 0; i < entryCount; i++) {
            clientMap.get(i);
        }

        assertTrueEventually(() -> {
            // 10. We should have 2 connection at final state
            // since there will be 2 members in each member group.
            makeSureConnectedToServers(client, 2);

            SubsetMembersView subsetMembersViewAfterTerminate = subsetMembers.getSubsetMembersView();
            assertNotNull(subsetMembersViewAfterTerminate);

            // 11. We should not have previous group A's member uuids in latest MemberGroupsView state
            assertNotNull(subsetMembersViewAfterTerminate.members());
            Collection<UUID> allUuidsAfterTermination = subsetMembersViewAfterTerminate.members();
            for (UUID member : subsetMemberUuidsBeforeTerminationOfGroupA) {
                assertFalse(allUuidsAfterTermination.contains(member));
            }
        });
    }


    @Test
    public void connected_to_subset_of_servers() {
        Config configA = newConfigWithAttribute("A");
        List<HazelcastInstance> groupA = initNodes(3, configA);

        Config configB = newConfigWithAttribute("B");
        List<HazelcastInstance> groupB = initNodes(3, configB);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false).getSubsetRoutingConfig().setEnabled(true)
                    .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap map = client.getMap("connected_to_subset_of_servers");
        for (int i = 0; i < 10_000; i++) {
            map.set(i, i);
        }

        assertEquals(10_000, map.size());
        makeSureConnectedToServers(client, 3);
    }

    @Test
    public void testFullGroupFailover_WhenCurrentGroupKilled() {
        final int nodesPerGroup = 2;

        List<HazelcastInstance> groupA = initNodes(nodesPerGroup, newConfigWithAttribute("A"));
        List<HazelcastInstance> groupB = initNodes(nodesPerGroup, newConfigWithAttribute("B"));
        List<HazelcastInstance> groupC = initNodes(nodesPerGroup, newConfigWithAttribute("C"));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false).getSubsetRoutingConfig().setEnabled(true)
                    .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // assert client is connected to 1 group's worth of members
        SubsetMembers members = getHazelcastClientInstanceImpl(client).getClientClusterService().getSubsetMembers();
        assertSubsetMembersSize(client, nodesPerGroup);
        makeSureConnectedToServers(client, nodesPerGroup);
        Set<UUID> subsetUUIDs = members.getSubsetMembersView().members();

        // determine which group the client is connected to
        List<HazelcastInstance> toKillInstances = findMatchingGroup(subsetUUIDs, groupA, groupB, groupC);

        // kill all members in this group
        Set<UUID> killedUUIDs = toKillInstances.stream().map(instance -> instance.getCluster().getLocalMember().getUuid())
                                               .collect(Collectors.toSet());
        for (HazelcastInstance instance : toKillInstances) {
            instance.getLifecycleService().terminate();
        }

        assertTrueEventually(() -> {
            // verify the client has connected to all members of 1 of the remaining groups
            assertSubsetMembersSize(client, nodesPerGroup);
            assertNotContainsAll(members.getSubsetMembersView().members(), killedUUIDs);
            for (UUID uuid : killedUUIDs) {
                makeSureDisconnectedFromServer(client, uuid);
            }
            makeSureConnectedToServers(client, nodesPerGroup);
        });
    }

    @SafeVarargs
    private List<HazelcastInstance> findMatchingGroup(Set<UUID> matchAgainst, List<HazelcastInstance>... toCheck) {
        for (List<HazelcastInstance> group : toCheck) {
            if (group.stream().allMatch(instance -> matchAgainst.contains(instance.getCluster().getLocalMember().getUuid()))) {
                return group;
            }
        }
        throw new IllegalStateException("No subset of members found which matches required UUIDs: " + matchAgainst);
    }

    @Test
    public void testClientListenerDisconnected() {
        Config config = new Config();
        config.setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "1");

        final HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        final HazelcastInstance hz2 = hazelcastFactory.newHazelcastInstance(config);

        int clientCount = 10;
        int disconnectCount = clientCount;
        ClientServiceTest.ClientDisconnectedListenerLatch listenerLatch
                = new ClientServiceTest.ClientDisconnectedListenerLatch(disconnectCount);
        hz.getClientService().addClientListener(listenerLatch);
        hz2.getClientService().addClientListener(listenerLatch);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false).getSubsetRoutingConfig().setEnabled(true)
                    .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);

        Collection<HazelcastInstance> clients = new LinkedList<>();
        for (int i = 0; i < clientCount; i++) {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
            IMap<Object, Object> map = client.getMap(randomMapName());

            map.addEntryListener(new EntryAdapter<Object, Object>(), true);
            map.put(generateKeyOwnedBy(hz), "value");
            map.put(generateKeyOwnedBy(hz2), "value");

            clients.add(client);
        }

        ExecutorService ex = Executors.newFixedThreadPool(4);
        try {
            for (final HazelcastInstance client : clients) {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        client.shutdown();
                    }
                });
            }

            assertOpenEventually("Not all disconnected events arrived", listenerLatch);

            assertTrueEventually("First server still have connected clients", new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(0, hz.getClientService().getConnectedClients().size());
                }
            });
            assertTrueEventually("Second server still have connected clients", new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(0, hz2.getClientService().getConnectedClients().size());
                }
            });
        } finally {
            ex.shutdown();
        }
    }

    private List<HazelcastInstance> initNodes(int amount, Config config) {
        List<HazelcastInstance> list = new ArrayList<>(amount);
        for (int k = 0; k < amount; k++) {
            list.add(hazelcastFactory.newHazelcastInstance(config));
        }
        return list;
    }

    private void assertSubsetMembersSize(HazelcastInstance client, int size) {
        assertTrueEventually(() -> {
            SubsetMembers members = getHazelcastClientInstanceImpl(client).getClientClusterService().getSubsetMembers();
            // Client might be disconnected from cluster if all members were killed, so new view is not yet ready
            if (members.getSubsetMembersView() == null) {
                throw new AssertionError("SubsetMembersView is null!");
            }
            assertEquals(size, members.getSubsetMembersView().members().size());
        });
    }

    @NotNull
    private Config newConfigWithAttribute(String attribute) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        configureNodeAware(config);
        config.setProperty("hazelcast.client.internal.push.period.seconds", "2");
        config.getMemberAttributeConfig()
              .setAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE, attribute);
        return config;
    }

    private void configureNodeAware(Config config) {
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        partitionGroupConfig
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.NODE_AWARE);
    }
}
