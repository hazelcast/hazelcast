/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.impl.operations.DemoteDataMemberOp;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.DEMOTE_DATA_MEMBER;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class DemoteDataMemberTest extends HazelcastTestSupport {

    @Test
    public void dataMaster_demoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertFalse(getMember(hz1).isLiteMember());
        assertTrue(getMember(hz2).isLiteMember());
        assertTrue(getMember(hz3).isLiteMember());
    }

    @Test
    public void dataMember_demoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertTrue(getMember(hz1).isLiteMember());
        assertFalse(getMember(hz2).isLiteMember());
        assertTrue(getMember(hz3).isLiteMember());
    }

    @Test
    public void liteMember_demotion_shouldFail_onLocal() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void liteMember_demotion_shouldFail_onNonMaster() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz3));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .rootCause().isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void liteMember_demotion_shouldBeNoop_onMaster() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        future.join();
    }

    @Test
    public void notExistingMember_demotion_shouldFail() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(UuidUtil.newUnsecureUUID());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .rootCause().isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void standaloneDataMember_demoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance(new Config());

        assertThatThrownBy(() -> hz.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertFalse(getMember(hz).isLiteMember());
    }

    @Test
    public void demotedMasterDataMember_shouldHave_partitionsMigrated() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz1);

        hz1.getCluster().demoteLocalDataMember();

        assertNoPartitionsAssigned(hz1);
        waitAllForSafeState(Arrays.asList(hz1, hz2, hz3));

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void demotedDataMember_shouldHave_partitionsMigrated() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz2);

        hz2.getCluster().demoteLocalDataMember();

        assertNoPartitionsAssignedEventually(hz2);

        waitAllForSafeState(hz1, hz2, hz3);

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void dataMember_shouldFailWhenOnlyMember() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());

        int entryCount = 100;
        String mapName = randomMapName();
        IMap<String, String> testMap = hz1.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }
        assertEquals(entryCount, testMap.size());
        assertPartitionsAssigned(hz1);

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertPartitionsAssigned(hz1);
        assertFalse(getMember(hz1).isLiteMember());
    }

    @Test
    public void dataMember_shouldFailWhenLastDataMember() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz2);

        assertTrue(hz1.getPartitionService().isLocalMemberSafe());
        assertTrue(hz1.getPartitionService().isClusterSafe());

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertPartitionsAssigned(hz2);
        assertFalse(getMember(hz2).isLiteMember());
    }

    @Test
    public void demotion_shouldFail_whenClusterStatePassive() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.PASSIVE);
    }

    @Test
    public void demotion_shouldFail_whenClusterStateFrozen() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.FROZEN);
    }

    @Test
    public void demotion_shouldFail_whenClusterStateNoMigration() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.NO_MIGRATION);
    }

    private void demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState state) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        changeClusterStateEventually(hz2, state);

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void demotion_shouldFail_whenMastershipClaimInProgress_duringDemotion() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        // artificially set mastership claim flag
        ClusterServiceImpl clusterService = getNode(hz1).getClusterService();
        clusterService.getClusterJoinManager().setMastershipClaimInProgress();

        Cluster cluster = hz2.getCluster();
        assertThatThrownBy(cluster::demoteLocalDataMember)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void demotion_shouldSucceed_whenMasterLeaves_duringDemotion() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        assertClusterSizeEventually(3, hz2);

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(DEMOTE_DATA_MEMBER));
        Cluster cluster = hz3.getCluster();
        Future<Exception> future = spawn(() -> {
            try {
                cluster.demoteLocalDataMember();
            } catch (Exception e) {
                return e;
            }
            return null;
        });
        assertDemotionInvocationStarted(hz3);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(2, hz2, hz3);

        Exception exception = future.get();
        assertNull(exception);
        assertTrue(getMember(hz3).isLiteMember());
        assertNoPartitionsAssigned(hz3);
    }

    @Test
    public void demotion_shouldSucceed_whenMasterIsSuspected_duringDemotion() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        assertClusterSizeEventually(3, hz2);

        rejectOperationsBetween(hz3, hz1, F_ID, asList(DEMOTE_DATA_MEMBER, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz2, F_ID, asList(MEMBER_INFO_UPDATE, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz1, F_ID, singletonList(HEARTBEAT));

        Cluster cluster = hz3.getCluster();
        Future future = spawn(cluster::demoteLocalDataMember);

        assertDemotionInvocationStarted(hz3);

        suspectMember(getNode(hz3), getNode(hz1));
        suspectMember(getNode(hz2), getNode(hz1));

        assertMasterAddressEventually(getAddress(hz2), hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(EXPLICIT_SUSPICION));

        future.get();

        assertTrue(hz3.getCluster().getLocalMember().isLiteMember());
        assertNoPartitionsAssigned(hz3);
    }

    @Test
    public void test_data_member_demotion_causes_no_data_loss_on_three_members() throws InterruptedException {
        int entryCount = 1000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();

        // start first hazelcast instance as a data member
        HazelcastInstance firstHazelcastInstance = factory.newHazelcastInstance(config);

        // start second and third hazelcast instances as a data member
        HazelcastInstance secondHazelcastInstance = factory.newHazelcastInstance(config);
        HazelcastInstance thirdHazelcastInstance = factory.newHazelcastInstance(config);

        // check if cluster is in a good shape
        assertTrueEventually(() -> assertTrue(firstHazelcastInstance.getPartitionService().isClusterSafe()));

        // insert some dummy data into the testing map
        String mapName = randomMapName();
        IMap<String, String> testMap = firstHazelcastInstance.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }

        // check all data is correctly inserted
        assertEquals(entryCount, testMap.size());

        // demote second instance
        secondHazelcastInstance.getCluster().demoteLocalDataMember();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHazelcastInstance.getMap(mapName).size()));
        assertTrueEventually(() -> assertEquals(entryCount, thirdHazelcastInstance.getMap(mapName).size()));
    }

    @Test
    public void test_data_member_demotion_causes_no_data_loss_on_two_members() throws InterruptedException {
        int entryCount = 1000;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();

        // start first hazelcast instance as a data member
        HazelcastInstance firstHazelcastInstance = factory.newHazelcastInstance(config);

        // start second hazelcast instance as a data member
        HazelcastInstance secondHazelcastInstance = factory.newHazelcastInstance(config);

        // check if cluster is in a good shape
        assertTrueEventually(() -> assertTrue(firstHazelcastInstance.getPartitionService().isClusterSafe()));

        // insert some dummy data into the testing map
        String mapName = randomMapName();
        IMap<String, String> testMap = firstHazelcastInstance.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }

        // check all data is correctly inserted
        assertEquals(entryCount, testMap.size());

        // kill second instance
        secondHazelcastInstance.getCluster().demoteLocalDataMember();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHazelcastInstance.getMap(mapName).size()));
    }

    private void assertDemotionInvocationStarted(HazelcastInstance instance) {
        OperationServiceImpl operationService = getNode(instance).getNodeEngine().getOperationService();
        InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();

        assertTrueEventually(() -> {
            for (Map.Entry<Long, Invocation> entry : invocationRegistry.entrySet()) {
                if (entry.getValue().op instanceof DemoteDataMemberOp) {
                    return;
                }
            }
            fail("Cannot find DemoteDataMemberOp invocation!");
        });
    }

    private static void assertPartitionsAssigned(HazelcastInstance instance) {
        Address address = getAddress(instance);
        InternalPartition[] partitions = getPartitionService(instance).getInternalPartitions();

        int k = 0;
        for (InternalPartition partition : partitions) {
            if (address.equals(partition.getOwnerOrNull())) {
                k++;
            }
        }
        assertThat(k).isGreaterThan(0);
    }

    private static void assertNoPartitionsAssigned(HazelcastInstance instance) {
        Address address = getAddress(instance);
        InternalPartition[] partitions = getPartitionService(instance).getInternalPartitions();
        for (InternalPartition partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                assertNotEquals("Replica " + i + " of partition " + partition + " must not have the given address",
                        address, partition.getReplicaAddress(i));
            }
        }
    }

    private static void assertNoPartitionsAssignedEventually(HazelcastInstance instance) {
        assertTrueEventually(() -> assertNoPartitionsAssigned(instance));
    }

    private static Member getMember(HazelcastInstance hz) {
        return hz.getCluster().getLocalMember();
    }
}
