/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaChangeEvent;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.tcp.DelegatingConnectionManager;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ClusterWidePartitionStateIntegrityTest extends HazelcastTestSupport {

    private static final boolean MOCK_NETWORK = false;

    private TestHazelcastInstanceFactory factory;

    private final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    @Before
    public void setup() {
        if (MOCK_NETWORK) {
            factory = createHazelcastInstanceFactory();
        }
    }

    @After
    public void tearDown() {
        if (!MOCK_NETWORK) {
            HazelcastInstanceManager.terminateAll();
        }
    }

    @Test
    public void test() throws InterruptedException {
        final int nodeCount = 5;

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "13");
        initNetworkConfig(config, nodeCount);

        Config liteConfig = new Config();
        liteConfig.setProperty(GroupProperty.PARTITION_COUNT, "13");
        liteConfig.setLiteMember(true);
        initNetworkConfig(liteConfig, nodeCount);

        final HazelcastInstance master = newHazelcastInstance(config);
        final HazelcastInstance lite = newHazelcastInstance(liteConfig);

        InternalPartitionService masterPartitionService = getPartitionService(master);
        masterPartitionService.firstArrangement();

        final int initialPartitionStateVersion = masterPartitionService.getPartitionStateVersion();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                InternalPartitionService partitionService = getPartitionService(lite);
                assertEquals(initialPartitionStateVersion, partitionService.getPartitionStateVersion());
            }
        });

        blockedAddresses.add(getAddress(lite));

        final HazelcastInstance member1 = newHazelcastInstance(config);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance instance : getAllHazelcastInstances()) {
                    InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
                    assertTrue(partitionService.isReplicaSyncAllowed());
                    assertFalse(partitionService.hasOnGoingMigrationLocal());
                }
            }
        });

        final int finalPartitionStateVersion = masterPartitionService.getPartitionStateVersion();
        final InternalPartition[] finalPartitions = masterPartitionService.getPartitions();

        final InternalPartitionServiceImpl member1PartitionService = (InternalPartitionServiceImpl) getPartitionService(member1);
        assertEquals(finalPartitionStateVersion, member1PartitionService.getPartitionStateVersion());
        assertPartitionsTableEqual(finalPartitions, member1PartitionService.getPartitions());


        final Collection<PartitionReplicaChangeEvent> lostEvents
                = Collections.synchronizedCollection(new HashSet<PartitionReplicaChangeEvent>());
        final InternalPartitionServiceImpl litePartitionService = (InternalPartitionServiceImpl) getPartitionService(lite);
        litePartitionService.addPartitionListener(new PartitionListener() {
            @Override
            public void replicaChanged(PartitionReplicaChangeEvent event) {
                if (event.getNewAddress() == null && event.getReplicaIndex() == 0) {
                    System.err.println("event = " + event);
                    lostEvents.add(event);
                }
            }
        });

        TestUtil.terminateInstance(master);
        blockedAddresses.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                InternalPartition[] partitionsRef = litePartitionService.getPartitions();
                int partitionStateVersionRef = litePartitionService.getPartitionStateVersion();

                InternalPartition[] partitions1 = member1PartitionService.getPartitions();
                int version1 = member1PartitionService.getPartitionStateVersion();

                assertThat(version1, Matchers.greaterThanOrEqualTo(finalPartitionStateVersion));
                assertEquals(partitionStateVersionRef, version1);
                assertPartitionsTableEqual(partitionsRef, partitions1);
            }
        });

        assertEquals(lostEvents.toString(), 0, lostEvents.size());
    }

    private Collection<HazelcastInstance> getAllHazelcastInstances() {
        return MOCK_NETWORK ? factory.getAllHazelcastInstances() : Hazelcast.getAllHazelcastInstances();
    }

    private HazelcastInstance newHazelcastInstance(Config config) {
        final HazelcastInstance instance;
        if (MOCK_NETWORK) {
            instance = factory.newHazelcastInstance();
            final Node node = getNode(instance);
            FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
            cm.setPacketFilter(new PartitionStatePacketFilter(node.getSerializationService(), blockedAddresses));
        } else {
            return HazelcastInstanceManager
                    .newHazelcastInstance(config, null, new PartitionStateNodeContext(blockedAddresses));
        }
        return instance;
    }

    private static void assertPartitionsTableEqual(InternalPartition[] partitions1, InternalPartition[] partitions2) {
        assertEquals(partitions1.length, partitions2.length);

        for (int i = 0; i < partitions1.length; i++) {
            InternalPartition partition1 = partitions1[i];
            InternalPartition partition2 = partitions2[i];

            assertPartitionEquals(partition1, partition2);
        }
    }

    private static void assertPartitionEquals(InternalPartition partition1, InternalPartition partition2) {
        for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
            Address address1 = partition1.getReplicaAddress(i);
            Address address2 = partition2.getReplicaAddress(i);
            assertAddressEquals(address1, address2);
        }
    }

    private static void assertAddressEquals(Address address1, Address address2) {
        if (address1 == null) {
            assertNull(address2);
        } else {
            assertEquals(address1, address2);
        }
    }

    private static void initNetworkConfig(Config config, int nodeCount) {
        if (MOCK_NETWORK) {
            return;
        }
        final JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        final TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true).clear();

        for (int i = 0; i < nodeCount; i++) {
            tcpIpConfig.addMember("127.0.0.1:" + (5701 + i));
        }
    }

    private static class PartitionStateNodeContext extends DefaultNodeContext {

        private final Set<Address> blockedAddresses;

        private PartitionStateNodeContext(Set<Address> blockedAddresses) {
            this.blockedAddresses = blockedAddresses;
        }

        @Override
        public ConnectionManager createConnectionManager(final Node node, ServerSocketChannel serverSocketChannel) {
            final ConnectionManager connectionManager = super.createConnectionManager(node, serverSocketChannel);
            return new DelegatingConnectionManager(connectionManager) {

                final PacketFilter filter = new PartitionStatePacketFilter(node.getSerializationService(),
                        blockedAddresses);

                @Override
                public boolean transmit(Packet packet, Connection connection) {
                    return connection != null && filter.allow(packet, connection.getEndPoint()) && super
                            .transmit(packet, connection);
                }

                @Override
                public boolean transmit(Packet packet, Address target) {
                    return filter.allow(packet, target) && super.transmit(packet, target);
                }
            };
        }
    }

    private static class PartitionStatePacketFilter implements PacketFilter {
        final SerializationService serializationService;
        final Set<Address> blockedAddresses;

        public PartitionStatePacketFilter(SerializationService serializationService, Set<Address> blockedAddresses) {
            this.serializationService = serializationService;
            this.blockedAddresses = blockedAddresses;
        }

        @Override
        public boolean allow(Packet packet, Address endpoint) {
            return !packet.isHeaderSet(Packet.HEADER_OP) || allowOperation(packet, endpoint);
        }

        private boolean allowOperation(Packet packet, Address endpoint) {
            if (!blockedAddresses.contains(endpoint)) {
                return true;
            }

            try {
                String className = readOperationClassName(packet);
                if (PartitionStateOperation.class.getName().equals(className)) {
                    //                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    return false;
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return true;
        }

        /**
         * see {@link com.hazelcast.nio.IOUtil#extractOperationCallId(Data, SerializationService)}
         */
        private String readOperationClassName(Packet packet) throws IOException {
            ObjectDataInput input = serializationService.createObjectDataInput(packet);
            boolean identified = input.readBoolean();
            if (identified) {
                return null;
            }
            return input.readUTF();
        }
    }
}
