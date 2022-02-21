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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NoMigrationClusterStateTest extends HazelcastTestSupport {

    private final NoReplicationService service = new NoReplicationService();

    @Test
    public void rebalancing_shouldNotHappen_whenMemberLeaves() {
        Config config = newConfigWithMigrationAwareService();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        assertTrueAllTheTime(new AssertTask() {
            Node node = getNode(hz);
            InternalPartitionService partitionService = node.getPartitionService();

            @Override
            public void run() {
                List<Integer> memberPartitions = partitionService.getMemberPartitions(node.getThisAddress());
                assertThat(memberPartitions, empty());
                service.assertNoReplication();
            }
        }, 10);
    }

    @Test
    public void promotions_shouldHappen_whenMemberLeaves() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(new Config(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1]);
        assertAllPartitionsAreAssigned(instances[1], 1);

        assertClusterSizeEventually(2, instances[2]);
        assertAllPartitionsAreAssigned(instances[2], 1);

        assertEquals(getPartitionService(instances[1]).getPartitionStateStamp(),
                getPartitionService(instances[2]).getPartitionStateStamp());
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toAvailableMembers() {
        int clusterSize = MAX_REPLICA_COUNT + 3;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance[] instances = factory.newInstances(new Config(), clusterSize);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            terminateInstance(instances[i]);
        }

        for (int i = MAX_REPLICA_COUNT; i < clusterSize; i++) {
            assertClusterSizeEventually(clusterSize - MAX_REPLICA_COUNT, instances[i]);
            assertAllPartitionsAreAssigned(instances[i], 1);
        }
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toAvailableMembers_whenMembersRemovedWhenClusterPASSIVE() {
        int clusterSize = MAX_REPLICA_COUNT + 3;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance[] instances = factory.newInstances(new Config(), clusterSize);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.PASSIVE);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            terminateInstance(instances[i]);
        }

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.NO_MIGRATION);

        for (int i = MAX_REPLICA_COUNT; i < clusterSize; i++) {
            assertClusterSizeEventually(clusterSize - MAX_REPLICA_COUNT, instances[i]);
            assertAllPartitionsAreAssigned(instances[i], 1);
        }
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toNewMembers() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance[] instances = factory.newInstances(new Config(), MAX_REPLICA_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);

        HazelcastInstance[] newInstances = factory.newInstances(new Config(), 3);

        for (HazelcastInstance instance : newInstances) {
            assertClusterSizeEventually(MAX_REPLICA_COUNT + newInstances.length, instance);
        }

        for (HazelcastInstance instance : instances) {
            terminateInstance(instance);
        }

        for (HazelcastInstance instance : newInstances) {
            assertClusterSizeEventually(newInstances.length, instance);
            assertAllPartitionsAreAssigned(instance, newInstances.length);
        }

        long partitionStamp = getPartitionService(newInstances[0]).getPartitionStateStamp();
        for (HazelcastInstance instance : newInstances) {
            assertEquals(partitionStamp, getPartitionService(instance).getPartitionStateStamp());
        }
    }

    private static void assertAllPartitionsAreAssigned(HazelcastInstance instance, int replicaCount) {
        ClusterServiceImpl clusterService = getNode(instance).getClusterService();
        InternalPartitionService partitionService = getNode(instance).getPartitionService();

        assertTrueEventually(() -> {
            InternalPartition[] partitions = partitionService.getInternalPartitions();
            for (InternalPartition partition : partitions) {
                for (int i = 0; i < replicaCount; i++) {
                    Address owner = partition.getReplicaAddress(i);
                    assertNotNull(i + "th replica owner is null: " + partition, owner);
                    assertNotNull("No member for: " + owner, clusterService.getMember(owner));
                }
            }
        });
    }

    @Test
    public void backupReplication_shouldNotHappen_whenMemberLeaves() {
        Config config = newConfigWithMigrationAwareService();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        assertTrueAllTheTime(service::assertNoReplication, 10);
    }

    @Test
    public void rebalancing_shouldHappen_whenStateBecomesActive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(new Config(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        changeClusterStateEventually(instances[1], ClusterState.ACTIVE);
        assertAllPartitionsAreAssigned(instances[1], 2);
        assertAllPartitionsAreAssigned(instances[2], 2);
    }

    @Test
    public void rebalancing_shouldNotHappen_whenStateBecomesFrozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(newConfigWithMigrationAwareService(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        changeClusterStateEventually(instances[1], ClusterState.FROZEN);

        assertTrueAllTheTime(service::assertNoReplication, 10);
    }

    private Config newConfigWithMigrationAwareService() {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(new ServiceConfig()
                .setEnabled(true)
                .setName("no-replication-service")
                .setImplementation(service));
        return config;
    }

    private static class NoReplicationService implements MigrationAwareService {

        private final AtomicReference<AssertionError> replicationRequested = new AtomicReference<AssertionError>();

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            AssertionError error = new AssertionError("Replication requested: " + event);
            replicationRequested.compareAndSet(null, error);
            throw error;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
        }

        void assertNoReplication() {
            AssertionError error = replicationRequested.get();
            if (error != null) {
                throw error;
            }
        }
    }
}
