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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.config.ServicesConfig;
import com.hazelcast.internal.partition.service.TestAbstractMigrationAwareService;
import com.hazelcast.internal.partition.service.TestIncrementOperation;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.internal.partition.service.fragment.TestFragmentIncrementOperation;
import com.hazelcast.internal.partition.service.fragment.TestFragmentedMigrationAwareService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionReplicaVersionsView;
import static com.hazelcast.test.Accessors.getClusterService;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class PartitionCorrectnessTestSupport extends HazelcastTestSupport {

    protected static final int PARALLEL_REPLICATIONS = 10;
    private static final int BACKUP_SYNC_INTERVAL = 1;

    private static final String[] NAMESPACES = {"ns1", "ns2"};

    TestHazelcastInstanceFactory factory;

    @Parameterized.Parameter(0)
    public int backupCount;

    @Parameterized.Parameter(1)
    public int nodeCount;

    protected int partitionCount = 111;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(10);
    }

    void fillData(HazelcastInstance hz) {
        NodeEngine nodeEngine = getNode(hz).nodeEngine;
        OperationService operationService = nodeEngine.getOperationService();
        for (int i = 0; i < partitionCount; i++) {
            operationService.invokeOnPartition(null, new TestIncrementOperation(), i);
            for (String name : NAMESPACES) {
                operationService.invokeOnPartition(null, new TestFragmentIncrementOperation(name), i);
            }
        }
    }

    Collection<HazelcastInstance> startNodes(final Config config, int count) throws InterruptedException {
        if (count == 1) {
            return Collections.singleton(factory.newHazelcastInstance(config));
        } else {
            Collection<HazelcastInstance> instances = new ArrayList<>(count);
            final Collection<HazelcastInstance> syncInstances = Collections.synchronizedCollection(instances);

            final CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                new Thread(() -> {
                    HazelcastInstance instance = factory.newHazelcastInstance(config);
                    syncInstances.add(instance);
                    latch.countDown();
                }).start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            return instances;
        }
    }

    void startNodes(final Config config, Collection<Address> addresses) throws InterruptedException {
        if (addresses.size() == 1) {
            factory.newHazelcastInstance(addresses.iterator().next(), config);
        } else {
            final CountDownLatch latch = new CountDownLatch(addresses.size());
            for (final Address address : addresses) {
                new Thread(() -> {
                    factory.newHazelcastInstance(address, config);
                    latch.countDown();
                }).start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    Collection<Address> terminateNodes(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<>(factory.getAllHazelcastInstances());
        assertThat(instances.size(), greaterThanOrEqualTo(count));

        Collections.shuffle(instances);

        if (count == 1) {
            HazelcastInstance hz = instances.get(0);
            Address address = getNode(hz).getThisAddress();
            TestUtil.terminateInstance(hz);
            return Collections.singleton(address);
        } else {
            final CountDownLatch latch = new CountDownLatch(count);
            final Throwable[] error = new Throwable[1];
            Collection<Address> addresses = new HashSet<>();

            for (int i = 0; i < count; i++) {
                final HazelcastInstance hz = instances.get(i);
                addresses.add(getNode(hz).getThisAddress());

                new Thread(() -> {
                    try {
                        TestUtil.terminateInstance(hz);
                    } catch (Throwable e) {
                        error[0] = e;
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            if (error[0] != null) {
                ExceptionUtil.sneakyThrow(error[0]);
            }
            return addresses;
        }
    }

    void assertPartitionAssignments() {
        assertPartitionAssignments(factory);
    }

    static void assertPartitionAssignments(TestHazelcastInstanceFactory factory) {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final int replicaCount = Math.min(instances.size(), InternalPartition.MAX_REPLICA_COUNT);

        HazelcastInstance master = null;
        for (HazelcastInstance hz : instances) {
            if (getClusterService(hz).isMaster()) {
                master = hz;
                break;
            }
        }
        assertNotNull(master);

        InternalPartitionService masterPartitionService = getPartitionService(master);
        InternalPartition[] masterPartitions = masterPartitionService.getInternalPartitions();

        for (HazelcastInstance hz : instances) {
            Node node = getNode(hz);
            InternalPartition[] partitions = node.getPartitionService().getInternalPartitions();
            ClusterService clusterService = node.getClusterService();
            Member localMember = node.getLocalMember();

            for (InternalPartition partition : partitions) {
                assertEquals("On " + localMember + ", Partition " + partition.getPartitionId() + " versions don't match: " + partition,
                        masterPartitions[partition.getPartitionId()].version(), partition.version());

                for (int i = 0; i < replicaCount; i++) {
                    PartitionReplica replica = partition.getReplica(i);
                    assertNotNull("On " + localMember + ", Replica " + i + " is not found in " + partition, replica);
                    assertNotNull("On " + localMember + ", Not member: " + replica,
                            clusterService.getMember(replica.address(), replica.uuid()));
                }
            }

            assertEquals(masterPartitionService.getPartitionStateStamp(), node.getPartitionService().getPartitionStateStamp());
        }
    }

    void assertSizeAndDataEventually() {
        assertSizeAndDataEventually(false);
    }

    void assertSizeAndDataEventually(boolean allowDirty) {
        assertTrueEventually(new AssertSizeAndDataTask(allowDirty));
    }

    void assertSizeAndData() {
        assertSizeAndData(false);
    }

    private void assertSizeAndData(boolean allowDirty) {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final int actualBackupCount = Math.min(backupCount, instances.size() - 1);
        final int expectedSize = partitionCount * (actualBackupCount + 1);

        assertPartitionAssignments();

        int total = 0;
        int[] fragmentTotals = new int[NAMESPACES.length];
        for (HazelcastInstance hz : instances) {
            TestMigrationAwareService service = getService(hz, TestMigrationAwareService.SERVICE_NAME);
            total += service.size();

            TestFragmentedMigrationAwareService fragmentedService
                    = getService(hz, TestFragmentedMigrationAwareService.SERVICE_NAME);
            for (int i = 0; i < NAMESPACES.length; i++) {
                fragmentTotals[i] += fragmentedService.size(NAMESPACES[i]);
            }

            Node node = getNode(hz);
            InternalPartitionService partitionService = node.getPartitionService();
            InternalPartition[] partitions = partitionService.getInternalPartitions();
            PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());

            // find leaks
            assertNoLeakingData(service, partitions, localReplica, null);
            for (String name : NAMESPACES) {
                assertNoLeakingData(fragmentedService, partitions, localReplica, name);
            }

            // find missing
            assertNoMissingData(service, partitions, localReplica, null);
            for (String name : NAMESPACES) {
                assertNoMissingData(fragmentedService, partitions, localReplica, name);
            }

            // check replica versions and values
            assertReplicaVersionsAndBackupValues(actualBackupCount, service, node, partitions, null, allowDirty);
            for (String name : NAMESPACES) {
                assertReplicaVersionsAndBackupValues(actualBackupCount, fragmentedService, node, partitions, name, allowDirty);
            }

            Address thisAddress = node.getThisAddress();
            assertMigrationEvents(service, thisAddress);
            assertMigrationEvents(fragmentedService, thisAddress);
        }

        assertEquals("Missing data!", expectedSize, total);
        for (int fragmentTotal : fragmentTotals) {
            assertEquals("Missing data!", expectedSize, fragmentTotal);
        }
    }

    private <N> void assertNoLeakingData(TestAbstractMigrationAwareService<N> service, InternalPartition[] partitions,
                                         PartitionReplica localReplica, N ns) {
        for (Integer p : service.keys(ns)) {
            int replicaIndex = partitions[p].getReplicaIndex(localReplica);
            assertThat("Partition: " + p + " is leaking on " + localReplica,
                    replicaIndex, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(backupCount)));
        }
    }

    private <N> void assertNoMissingData(TestAbstractMigrationAwareService<N> service, InternalPartition[] partitions,
                                         PartitionReplica localReplica, N ns) {
        for (InternalPartition partition : partitions) {
            int replicaIndex = partition.getReplicaIndex(localReplica);
            if (replicaIndex >= 0 && replicaIndex <= backupCount) {
                assertTrue("Partition: " + partition.getPartitionId() + ", replica: " + replicaIndex
                                + " data is missing on " + localReplica,
                        service.contains(ns, partition.getPartitionId()));
            }
        }
    }

    private <N> void assertReplicaVersionsAndBackupValues(int actualBackupCount, TestAbstractMigrationAwareService<N> service,
                                                            Node node, InternalPartition[] partitions, N name, boolean allowDirty) {
        Address thisAddress = node.getThisAddress();
        ServiceNamespace namespace = service.getNamespace(name);

        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                int partitionId = partition.getPartitionId();
                long[] replicaVersions = getPartitionReplicaVersionsView(node, partitionId).getVersions(namespace);

                for (int replica = 1; replica <= actualBackupCount; replica++) {
                    Address address = partition.getReplicaAddress(replica);
                    assertNotNull("On " + thisAddress + ", Replica: " + replica + " is not found in " + partition, address);

                    HazelcastInstance backupInstance = factory.getInstance(address);
                    assertNotNull("Instance for " + address + " is not found! -> " + partition, backupInstance);

                    Node backupNode = getNode(backupInstance);
                    assertNotNull(backupNode);

                    PartitionReplicaVersionsView backupReplicaVersionsView
                            = getPartitionReplicaVersionsView(backupNode, partitionId);
                    long[] backupReplicaVersions = backupReplicaVersionsView.getVersions(namespace);
                    assertNotNull(namespace + " replica versions are null on " + backupNode.address
                                    + ", partitionId: " + partitionId + ", replicaIndex: " + replica, backupReplicaVersions);

                    for (int i = replica - 1; i < actualBackupCount; i++) {
                        assertEquals("Replica version mismatch! Owner: " + thisAddress + ", Backup: " + address
                                        + ", Partition: " + partition + ", Replica: " + (i + 1) + " owner versions: "
                                        + Arrays.toString(replicaVersions) + " backup versions: "
                                        + Arrays.toString(backupReplicaVersions),
                                replicaVersions[i], backupReplicaVersions[i]);
                    }

                    if (!allowDirty) {
                        assertFalse("Backup replica is dirty! Owner: " + thisAddress + ", Backup: " + address
                                + ", Partition: " + partition, backupReplicaVersionsView.isDirty(namespace));
                    }

                    TestAbstractMigrationAwareService backupService = getService(backupInstance, service.getServiceName());
                    assertEquals("Wrong data! Partition: " + partitionId + ", replica: " + replica + " on "
                                    + address + " has stale value! " + Arrays.toString(backupReplicaVersions),
                            service.get(name, partitionId), backupService.get(name, partitionId));
                }
            }
        }
    }

    private void assertMigrationEvents(TestAbstractMigrationAwareService service, Address thisAddress) {
        Collection<PartitionMigrationEvent> beforeEvents = service.getBeforeEvents();
        int beforeEventsCount = beforeEvents.size();
        Collection<PartitionMigrationEvent> commitEvents = service.getCommitEvents();
        int commitEventsCount = commitEvents.size();
        Collection<PartitionMigrationEvent> rollbackEvents = service.getRollbackEvents();
        int rollbackEventsCount = rollbackEvents.size();

        assertEquals("Invalid migration event count on " + thisAddress
                + "! Before: " + beforeEventsCount + ", Commit: " + commitEventsCount
                + ", Rollback: " + rollbackEventsCount, beforeEventsCount, commitEventsCount + rollbackEventsCount);

        Collection<PartitionMigrationEvent> beforeEventsCopy = new ArrayList<PartitionMigrationEvent>(beforeEvents);
        beforeEvents.removeAll(commitEvents);
        beforeEvents.removeAll(rollbackEvents);
        assertTrue("Remaining before events: " + beforeEvents, beforeEvents.isEmpty());

        commitEvents.removeAll(beforeEventsCopy);
        rollbackEvents.removeAll(beforeEventsCopy);
        assertTrue("Remaining commit events: " + commitEvents, commitEvents.isEmpty());
        assertTrue("Remaining rollback events: " + rollbackEvents, rollbackEvents.isEmpty());
    }

    private <S extends TestAbstractMigrationAwareService> S getService(HazelcastInstance hz, String serviceName) {
        Node node = getNode(hz);
        return node.nodeEngine.getService(serviceName);
    }

    Config getConfig(boolean withService, boolean antiEntropyEnabled) {
        Config config = getConfig();

        if (withService) {
            ServicesConfig servicesConfig = ConfigAccessor.getServicesConfig(config);
            servicesConfig.addServiceConfig(TestMigrationAwareService.createServiceConfig(backupCount));
            servicesConfig.addServiceConfig(TestFragmentedMigrationAwareService.createServiceConfig(backupCount));
        }

        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), String.valueOf(BACKUP_SYNC_INTERVAL));

        int parallelReplications = antiEntropyEnabled ? PARALLEL_REPLICATIONS : 0;
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), String.valueOf(parallelReplications));
        return config;
    }

    private class AssertSizeAndDataTask implements AssertTask {
        private final boolean allowDirty;

        AssertSizeAndDataTask(boolean allowDirty) {
            this.allowDirty = allowDirty;
        }

        @Override
        public void run() throws Exception {
            assertSizeAndData(allowDirty);
        }
    }
}
