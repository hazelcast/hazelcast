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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.IndeterminateOperationStateExceptionTest.BackupOperation;
import com.hazelcast.partition.IndeterminateOperationStateExceptionTest.SilentOperation;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicClusterStateTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void clusterState_isActive_whenInstancesStarted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void joinNotAllowed_whenClusterState_isFrozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);

        expectedException.expect(IllegalStateException.class);
        factory.newHazelcastInstance();
        fail("New node should not start when cluster state is: " + ClusterState.FROZEN);
    }

    @Test
    public void joinNotAllowed_whenClusterState_isPassive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);

        expectedException.expect(IllegalStateException.class);
        factory.newHazelcastInstance();
        fail("New node should not start when cluster state is: " + ClusterState.PASSIVE);
    }

    @Test
    public void joinAllowed_whenClusterState_isNoMigration() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        instances[instances.length - 1].getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        HazelcastInstance hz = factory.newHazelcastInstance();
        assertClusterSize(4, hz);
    }

    @Test
    public void joinAllowed_whenKnownMemberReJoins_whenClusterState_isFrozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz1 = instances[instances.length - 1];
        hz1.getCluster().changeClusterState(ClusterState.FROZEN);

        HazelcastInstance hz2 = instances[0];
        Address address = getNode(hz2).getThisAddress();
        hz2.getLifecycleService().terminate();

        hz2 = factory.newHazelcastInstance(address);

        assertClusterSizeEventually(3, hz1, hz2);
        assertEquals(NodeState.ACTIVE, getNode(hz2).getState());
    }

    @Test
    public void joinAllowed_whenKnownMemberReJoins_whenClusterState_isPassive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz1 = instances[instances.length - 1];
        hz1.getCluster().changeClusterState(ClusterState.PASSIVE);

        HazelcastInstance hz2 = instances[0];
        Address address = getNode(hz2).getThisAddress();
        hz2.getLifecycleService().terminate();

        hz2 = factory.newHazelcastInstance(address);

        assertClusterSizeEventually(3, hz1, hz2);
        assertEquals(NodeState.PASSIVE, getNode(hz2).getState());
    }

    @Test
    public void changeClusterState_toNoMigration_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        expectedException.expect(IllegalStateException.class);
        hz2.getCluster().changeClusterState(ClusterState.NO_MIGRATION);
    }

    @Test
    public void changeClusterState_toFrozen_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        expectedException.expect(IllegalStateException.class);
        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test
    public void changeClusterState_toPassive_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, hz);
        expectedException.expect(IllegalStateException.class);
        hz2.getCluster().changeClusterState(ClusterState.PASSIVE);
    }

    @Test
    public void changeClusterState_toActive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2, hz3);

        // try until member is removed and partition-service takes care of removal
        changeClusterStateEventually(hz3, ClusterState.ACTIVE);

        assertClusterState(ClusterState.ACTIVE, hz2, hz3);
    }

    @Test
    public void changeClusterState_toPassive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2, hz3);

        // try until member is removed and partition-service takes care of removal
        changeClusterStateEventually(hz3, ClusterState.PASSIVE);

        assertClusterState(ClusterState.PASSIVE, hz2, hz3);
    }

    @Test
    public void changeClusterState_toFrozen_makesNodeStates_Active() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_toPassive_makesNodeStates_Passive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertNodeState(instances, NodeState.PASSIVE);
    }

    @Test
    public void changeClusterState_fromPassiveToActive_makesNodeStates_Active() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_fromPassiveToFrozen_makesNodeStates_Active() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_transaction_mustBe_TWO_PHASE() {
        HazelcastInstance hz = createHazelcastInstance();
        TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.ONE_PHASE);

        expectedException.expect(IllegalArgumentException.class);
        hz.getCluster().changeClusterState(ClusterState.FROZEN, options);
    }

    @Test
    public void readOperations_succeed_whenClusterState_passive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        HazelcastInstance hz = instances[instances.length - 1];
        Map<Object, Object> map = hz.getMap(randomMapName());
        changeClusterStateEventually(hz, ClusterState.PASSIVE);
        map.get(1);
    }

    @Test(timeout = 300000)
    public void test_noMigration_whenNodeLeaves_onClusterState_FROZEN() {
        testNoMigrationWhenNodeLeaves(ClusterState.FROZEN);
    }

    @Test(timeout = 300000)
    public void test_noMigration_whenNodeLeaves_onClusterState_PASSIVE() {
        testNoMigrationWhenNodeLeaves(ClusterState.PASSIVE);
    }

    private void testNoMigrationWhenNodeLeaves(final ClusterState clusterState) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance other = factory.newHazelcastInstance();

        IMap<Object, Object> map = master.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        changeClusterStateEventually(master, clusterState);

        Address otherAddress = getAddress(other);

        other.shutdown();
        assertClusterSizeEventually(1, master);

        other = factory.newHazelcastInstance(otherAddress);

        assertClusterSizeEventually(2, master, other);

        other.shutdown();
    }

    @Test
    public void test_listener_registration_whenClusterState_PASSIVE() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance other = factory.newHazelcastInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 7 -> 1 added + 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
    }

    @Test
    public void test_listener_deregistration_whenClusterState_PASSIVE() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance other = factory.newHazelcastInstance();

        UUID registrationId = master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 7 -> 1 added + 1 from {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().removePartitionLostListener(registrationId);
        // Expected = 6 -> see {@link com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers* instances
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 6);
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 6);
    }

    @Test
    public void test_eventsDispatched_whenClusterState_PASSIVE() {
        System.setProperty(EventServiceImpl.EVENT_SYNC_FREQUENCY_PROP, "1");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance other = factory.newHazelcastInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getMap(randomMapName());

        assertTrueEventually(() -> assertEquals(1, getNodeEngineImpl(other).getProxyService().getProxyCount()));
    }

    @Test
    public void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_PASSIVE() throws Exception {
        pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState.PASSIVE);
    }

    @Test
    public void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_FROZEN() throws Exception {
        pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState.FROZEN);
    }

    private void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState state)
            throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        Future<Object> future = getOperationService(hz2).invokeOnTarget(null, new SilentOperation(),
                getAddress(hz1));

        changeClusterStateEventually(hz2, state);
        hz1.shutdown();

        expectedException.expect(MemberLeftException.class);
        future.get();
    }

    @Test
    public void backupOperation_shouldBeAllowed_whenClusterState_PASSIVE() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        int partitionId = getPartitionId(hz2);
        changeClusterStateEventually(hz1, ClusterState.PASSIVE);

        InternalCompletableFuture<Object> future = getOperationService(hz1).invokeOnPartition(null,
                new PrimaryAllowedDuringPassiveStateOperation(), partitionId);
        future.join();

        assertTrueEventually(() -> assertTrue(hz1.getUserContext().containsKey(BackupOperation.EXECUTION_DONE)));
    }

    private static void assertNodeState(HazelcastInstance[] instances, NodeState expectedState) {
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            assertEquals(expectedState, node.getState());
        }
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final String serviceName, final String topic, final int size) {
        assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, topic);
            assertEquals(size, registrations.size());
        });
    }

    private static class PrimaryAllowedDuringPassiveStateOperation extends Operation
            implements BackupAwareOperation, AllowedDuringPassiveState {

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 1;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new BackupOperation();
        }
    }
}
