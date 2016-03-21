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
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicClusterStateTest
        extends HazelcastTestSupport {

    @Test
    public void clusterState_isActive_whenInstancesStarted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void changeClusterState_from_Active_to_Frozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);

        assertClusterState(ClusterState.FROZEN, instances);
    }

    @Test
    public void changeClusterState_from_Active_to_Passive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);

        assertClusterState(ClusterState.PASSIVE, instances);
    }

    @Test
    public void changeClusterState_from_Frozen_to_Active() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);
        assertClusterState(ClusterState.FROZEN, instances);

        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void changeClusterState_from_Frozen_to_Passive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);
        assertClusterState(ClusterState.FROZEN, instances);

        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertClusterState(ClusterState.PASSIVE, instances);
    }

    @Test
    public void changeClusterState_from_Passive_to_Active() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertClusterState(ClusterState.PASSIVE, instances);

        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void changeClusterState_from_Passive_to_Frozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertClusterState(ClusterState.PASSIVE, instances);

        hz.getCluster().changeClusterState(ClusterState.FROZEN);
        assertClusterState(ClusterState.FROZEN, instances);
    }

    @Test(expected = IllegalStateException.class)
    public void joinNotAllowed_whenClusterState_isFrozen() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);

        factory.newHazelcastInstance();
        fail("New node should not start when cluster state is: " + ClusterState.FROZEN);
    }

    @Test(expected = IllegalStateException.class)
    public void joinNotAllowed_whenClusterState_isPassive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHazelcastInstance();
        }

        HazelcastInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);

        factory.newHazelcastInstance();
        fail("New node should not start when cluster state is: " + ClusterState.PASSIVE);
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

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz2);
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

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz2);
        assertEquals(NodeState.PASSIVE, getNode(hz2).getState());
    }

    @Test(expected = IllegalStateException.class)
    public void changeClusterState_toFrozen_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        hz2.getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test(expected = IllegalStateException.class)
    public void changeClusterState_toPassive_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, hz);
        hz2.getCluster().changeClusterState(ClusterState.PASSIVE);
    }

    @Test
    public void changeClusterState_toActive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2);
        assertClusterSizeEventually(2, hz3);

        // try until member is removed and partition-service takes care of removal
        changeClusterStateEventually(hz3, ClusterState.ACTIVE);

        assertClusterState(ClusterState.ACTIVE, hz2, hz3);
    }

    @Test
    public void changeClusterState_toPassive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance hz1 = instances[0];
        HazelcastInstance hz2 = instances[1];
        HazelcastInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2);
        assertClusterSizeEventually(2, hz3);

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

    @Test(expected = IllegalArgumentException.class)
    public void changeClusterState_transaction_mustBe_TWO_PHASE() {
        HazelcastInstance hz = createHazelcastInstance();
        hz.getCluster()
                .changeClusterState(ClusterState.FROZEN, new TransactionOptions().setTransactionType(TransactionType.LOCAL));
    }

    @Test
    public void readOperations_succeed_whenClusterState_passive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        HazelcastInstance hz = instances[instances.length - 1];
        Map map = hz.getMap(randomMapName());
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
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance other = factory.newHazelcastInstance();

        final IMap<Object, Object> map = master.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        changeClusterStateEventually(master, clusterState);

        final Address otherAddress = getAddress(other);

        other.shutdown();
        assertClusterSizeEventually(1, master);

        other = factory.newHazelcastInstance(otherAddress);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, other);

        other.shutdown();
    }

    @Test
    public void test_listener_registration_whenClusterState_PASSIVE() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance other = factory.newHazelcastInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 1);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 1);
    }

    @Test
    public void test_listener_deregistration_whenClusterState_PASSIVE() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance other = factory.newHazelcastInstance();

        final String registrationId = master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 1);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 1);

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().removePartitionLostListener(registrationId);

        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 0);
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 0);
    }

    @Test
    public void test_eventsDispatched_whenClusterState_PASSIVE() {
        System.setProperty(EventServiceImpl.EVENT_SYNC_FREQUENCY_PROP, "1");
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance master = factory.newHazelcastInstance();
        final HazelcastInstance other = factory.newHazelcastInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getMap(randomMapName());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, getNodeEngineImpl(other).getProxyService().getProxyCount());
            }
        });
    }

    private static void assertClusterState(ClusterState expectedState, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals(expectedState, instance.getCluster().getClusterState());
        }
    }

    private static void assertNodeState(HazelcastInstance[] instances, NodeState expectedState) {
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            assertEquals(expectedState, node.getState());
        }
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final String serviceName, final String topic, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final InternalEventService eventService = getNode(instance).getNodeEngine().getEventService();
                final Collection<EventRegistration> registrations =
                        eventService.getRegistrations(serviceName, topic);
                assertEquals(size, registrations.size());
            }
        });
    }

}
