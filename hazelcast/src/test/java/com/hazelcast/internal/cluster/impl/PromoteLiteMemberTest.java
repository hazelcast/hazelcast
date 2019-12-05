/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.PROMOTE_LITE_MEMBER;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.test.PacketFiltersUtil.rejectOperationsBetween;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class PromoteLiteMemberTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void liteMaster_promoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        hz1.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz1).isLiteMember());
        assertAllNormalMembers(hz1.getCluster());

        assertAllNormalMembersEventually(hz2.getCluster());
        assertAllNormalMembersEventually(hz3.getCluster());
    }

    @Test
    public void liteMember_promoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        hz2.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz2).isLiteMember());
        assertAllNormalMembers(hz1.getCluster());

        assertAllNormalMembersEventually(hz2.getCluster());
        assertAllNormalMembersEventually(hz3.getCluster());
    }

    @Test
    public void normalMember_promotion_shouldFail_onLocal() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        exception.expect(IllegalStateException.class);
        hz1.getCluster().promoteLocalLiteMember();
    }

    @Test
    public void normalMember_promotion_shouldFail_onNonMaster() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz3));
        exception.expect(CompletionException.class);
        exception.expect(new RootCauseMatcher(IllegalStateException.class));
        future.join();
    }

    @Test
    public void normalMember_promotion_shouldBeNoop_onMaster() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        future.join();
    }

    @Test
    public void notExistingMember_promotion_shouldFail() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(UuidUtil.newUnsecureUUID());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        exception.expect(CompletionException.class);
        exception.expect(new RootCauseMatcher(IllegalStateException.class));
        future.join();
    }

    @Test
    public void standaloneLiteMember_promoted() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz = factory.newHazelcastInstance(new Config().setLiteMember(true));

        hz.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz).isLiteMember());
        assertAllNormalMembers(hz.getCluster());

        warmUpPartitions(hz);
        assertPartitionsAssigned(hz);
    }

    @Test
    public void promotedMasterLiteMember_shouldHave_partitionsAssigned() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertNoPartitionsAssigned(hz1);

        hz1.getCluster().promoteLocalLiteMember();

        assertPartitionsAssignedEventually(hz1);
    }

    @Test
    public void promotedLiteMember_shouldHave_partitionsAssigned() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertNoPartitionsAssigned(hz2);

        hz2.getCluster().promoteLocalLiteMember();

        assertPartitionsAssignedEventually(hz2);
    }

    @Test
    public void promotion_shouldFail_whenClusterStatePassive() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.PASSIVE);
    }

    @Test
    public void promotion_shouldFail_whenClusterStateFrozen() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.FROZEN);
    }

    @Test
    public void promotion_shouldFail_whenClusterStateNoMigration() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.NO_MIGRATION);
    }

    private void promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState state) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        changeClusterStateEventually(hz2, state);

        exception.expect(IllegalStateException.class);
        hz2.getCluster().promoteLocalLiteMember();
    }

    @Test
    public void promotion_shouldFail_whenMastershipClaimInProgress_duringPromotion() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        // artificially set mastership claim flag
        ClusterServiceImpl clusterService = getNode(hz1).getClusterService();
        clusterService.getClusterJoinManager().setMastershipClaimInProgress();

        Cluster cluster = hz2.getCluster();
        exception.expect(IllegalStateException.class);
        cluster.promoteLocalLiteMember();
    }

    @Test
    public void promotion_shouldFail_whenMasterLeaves_duringPromotion() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(PROMOTE_LITE_MEMBER));
        final Cluster cluster = hz3.getCluster();
        Future<Exception> future = spawn(new Callable<Exception>() {
            @Override
            public Exception call() throws Exception {
                try {
                    cluster.promoteLocalLiteMember();
                } catch (Exception e) {
                    return e;
                }
                return null;
            }
        });
        assertPromotionInvocationStarted(hz3);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(2, hz2, hz3);

        Exception exception = future.get();
        // MemberLeftException is wrapped by HazelcastException
        assertInstanceOf(MemberLeftException.class, exception.getCause());
    }

    @Test
    public void promotion_shouldFail_whenMasterIsSuspected_duringPromotion() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance hz2 = factory.newHazelcastInstance(new Config());
        final HazelcastInstance hz3 = factory.newHazelcastInstance(new Config().setLiteMember(true));

        assertClusterSizeEventually(3, hz2);

        rejectOperationsBetween(hz3, hz1, F_ID, asList(PROMOTE_LITE_MEMBER, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz2, F_ID, asList(MEMBER_INFO_UPDATE, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz1, F_ID, singletonList(HEARTBEAT));

        final Cluster cluster = hz3.getCluster();
        Future future = spawn(new Runnable() {
            @Override
            public void run() {
                cluster.promoteLocalLiteMember();
            }
        });

        assertPromotionInvocationStarted(hz3);

        suspectMember(getNode(hz3), getNode(hz1));
        suspectMember(getNode(hz2), getNode(hz1));

        assertMasterAddressEventually(getAddress(hz2), hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(EXPLICIT_SUSPICION));
        try {
            future.get();
            fail("Promotion should fail!");
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalStateException.class, e.getCause());
        }
    }

    @Test
    public void masterMemberAttributes_arePreserved_afterPromotion() throws Exception {
        memberAttributes_arePreserved_afterPromotion(true);
    }

    @Test
    public void normalMemberAttributes_arePreserved_afterPromotion() throws Exception {
        memberAttributes_arePreserved_afterPromotion(false);
    }

    private void memberAttributes_arePreserved_afterPromotion(boolean isMaster) throws Exception {
        final String attribute1 = "attr1";
        final String attribute2 = "attr2";
        final String attributeValue = "value";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[2];
        instances[0] = factory.newHazelcastInstance(new Config().setLiteMember(isMaster));
        instances[1] = factory.newHazelcastInstance(new Config().setLiteMember(!isMaster));

        HazelcastInstance hz = instances[isMaster ? 0 : 1];

        // Get local member and SET attribute BEFORE promotion
        Member localMember = hz.getCluster().getLocalMember();
        localMember.setAttribute(attribute1, attributeValue);
        assertEquals(attributeValue, localMember.getAttribute(attribute1));

        // Promote local Lite member
        hz.getCluster().promoteLocalLiteMember();

        // Get local member and SET attribute AFTER promotion
        localMember = hz.getCluster().getLocalMember();
        localMember.setAttribute(attribute2, attributeValue);

        // Check attributes from localMember
        assertEquals(attributeValue, localMember.getAttribute(attribute1));
        assertEquals(attributeValue, localMember.getAttribute(attribute2));

        // Check attributes from member list
        for (Member member : hz.getCluster().getMembers()) {
            if (member.localMember()) {
                assertEquals(attributeValue, member.getAttribute(attribute1));
                assertEquals(attributeValue, member.getAttribute(attribute2));
                break;
            }
        }
    }

    private void assertPromotionInvocationStarted(HazelcastInstance instance) {
        final OperationServiceImpl operationService =
                (OperationServiceImpl) getNode(instance).getNodeEngine().getOperationService();
        final InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Map.Entry<Long, Invocation> entry : invocationRegistry.entrySet()) {
                    if (entry.getValue().op instanceof PromoteLiteMemberOp) {
                        return;
                    }
                }
                fail("Cannot find PromoteLiteMemberOp invocation!");
            }
        });
    }

    private static void assertPartitionsAssignedEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertPartitionsAssigned(instance);
            }
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
        assertThat(k, greaterThan(0));
    }

    private static void assertNoPartitionsAssigned(HazelcastInstance instance) {
        Address address = getAddress(instance);
        InternalPartition[] partitions = getPartitionService(instance).getInternalPartitions();
        for (InternalPartition partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                assertNotEquals(address, partition.getReplicaAddress(i));
            }
        }
    }

    private static void assertAllNormalMembers(Cluster cluster) {
        for (Member member : cluster.getMembers()) {
            assertFalse("Member is lite: " + member, member.isLiteMember());
        }
    }

    private static void assertAllNormalMembersEventually(final Cluster cluster) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertAllNormalMembers(cluster);
            }
        });
    }

    private static Member getMember(HazelcastInstance hz) {
        return hz.getCluster().getLocalMember();
    }
}
